use std::{collections::BTreeSet, sync::Arc};

use common::store::ObjectStoreExt;
use datasets_common::{name::Name, version::Version};
use futures::{StreamExt as _, TryStreamExt};
use metadata_db::MetadataDb;
use object_store::{ObjectStore, path::Path as ObjectStorePath};
use tokio::sync::OnceCell;

const PLACEHOLDER_OWNER: &'static str = "no-owner";

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
    initialized: Arc<OnceCell<()>>,
}

impl<S> DatasetManifestsStore<S>
where
    S: ObjectStore + Clone,
{
    /// Create a new [`DatasetManifestsStore`] instance with the given metadata database and underlying store.
    pub fn new(metadata_db: MetadataDb, store: S) -> Self {
        Self {
            metadata_db,
            store,
            initialized: Arc::new(OnceCell::new()),
        }
    }

    /// Initialize the manifests store by loading existing manifests into the metadata database
    ///
    /// This method ensures that all manifests present in the object store are registered in the
    /// metadata database. It's idempotent and will only register manifests that don't already exist.
    /// The initialization runs only once per instance using OnceCell.
    pub async fn init(&self) {
        self.initialized
            .get_or_init(|| async {
                self.init_metadata_db().await;
            })
            .await;
    }

    /// Get the latest version for a given dataset name
    ///
    /// Returns the latest version if found, or None if no dataset with the given name exists.
    pub async fn get_latest_version(
        &self,
        name: &Name,
    ) -> Result<Option<Version>, GetLatestVersionError> {
        self.init().await;

        let dataset = self
            .metadata_db
            .get_dataset_latest_version_with_details(name)
            .await
            .map_err(GetLatestVersionError)?;

        Ok(dataset.map(|details| details.version.into()))
    }

    /// Get all dataset manifests from the underlying store
    ///
    /// Returns a set of all available manifests with their name, version, and path information.
    /// This operation directly queries the underlying store without caching.
    pub async fn list(&self) -> Vec<(Name, Version)> {
        self.init().await;

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
        self.init().await;

        let res = match version.into() {
            None => self
                .metadata_db
                .get_dataset_latest_version_with_details(name)
                .await
                .map_err(GetError::MetadataDbError)?,
            Some(version) => self
                .metadata_db
                .get_dataset_with_details(name, version)
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
    pub async fn store<M>(
        &self,
        name: &Name,
        version: &Version,
        manifest: &M,
    ) -> Result<ObjectStorePath, StoreError>
    where
        M: serde::Serialize,
    {
        self.init().await;

        // Prepare manifest data for storage
        let manifest_json =
            serde_json::to_string(&manifest).map_err(StoreError::ManifestSerialization)?;

        // Store manifest in underlying store
        let manifest_path = ObjectStorePath::from(format!(
            "{}__{}.json",
            name,
            version.to_underscore_version()
        ));

        self.store
            .put(&manifest_path, manifest_json.into())
            .await
            .map_err(StoreError::ManifestStorage)?;

        Ok(manifest_path)
    }

    /// Internal method that performs the actual async initialization work
    async fn init_metadata_db(&self) {
        tracing::info!(
            "Initializing ManifestsStore: loading manifests from store into metadata database"
        );

        let manifests_list = list(&self.store).await;
        let mut total_count = 0;
        let mut registered_count = 0;
        let mut error_count = 0;

        for (name, version, path) in manifests_list {
            total_count += 1;

            // Check if the dataset already exists in the metadata database
            let dataset_exists = match self.metadata_db.dataset_exists(&name, &version).await {
                Ok(exists) => exists,
                Err(err) => {
                    error_count += 1;
                    tracing::warn!(
                        dataset_name = %name,
                        dataset_version = %version,
                        error = %err,
                        "Failed to check dataset existence in metadata database"
                    );
                    continue;
                }
            };

            if dataset_exists {
                tracing::debug!(
                    dataset_name = %name,
                    dataset_version = %version,
                    "Dataset already exists in metadata database, skipping"
                );
                continue;
            }

            // Register the dataset in the metadata database
            match self
                .metadata_db
                .register_dataset(PLACEHOLDER_OWNER, &name, &version, &path.to_string())
                .await
            {
                Ok(()) => {
                    registered_count += 1;
                    tracing::debug!(
                        dataset_name = %name,
                        dataset_version = %version,
                        manifest_path = %path,
                        "Successfully registered manifest in metadata database"
                    );
                }
                Err(err) => {
                    error_count += 1;
                    tracing::warn!(
                        dataset_name = %name,
                        dataset_version = %version,
                        manifest_path = %path,
                        error = %err,
                        "Failed to register manifest in metadata database"
                    );
                }
            }
        }

        if error_count > 0 {
            tracing::warn!(
                total_manifests = total_count,
                registered_manifests = registered_count,
                failed_registrations = error_count,
                "ManifestsStore initialization completed with some failures"
            );
        } else {
            tracing::info!(
                total_manifests = total_count,
                registered_manifests = registered_count,
                "Successfully initialized ManifestsStore with all manifests"
            );
        }
    }
}

async fn list<S>(store: &S) -> BTreeSet<(Name, Version, ManifestPath)>
where
    S: ObjectStore,
{
    let mut set = BTreeSet::new();

    let mut list_result = store.list(None);
    while let Some(file) = list_result.next().await {
        // Skip any errored file listing
        let file = match file {
            Ok(file) => file,
            Err(error) => {
                tracing::debug!(error = ?error, "Skipping errored file listing");
                continue;
            }
        };

        let manifest_path_str = file.location.to_string();
        let manifest_path = match ManifestPath::try_from(manifest_path_str.clone()) {
            Ok(path) => path,
            Err(err) => {
                tracing::debug!(path = %manifest_path_str, error = ?err, "Skipping file with unsupported format");
                continue;
            }
        };

        // Skip any path with no file name
        let Some(file_name) = manifest_path.as_ref().filename() else {
            tracing::debug!(path = %manifest_path, "Skipping path with no filename");
            continue;
        };

        let stem = file_name.trim_end_matches(".json");

        let (name_str, version_str) = stem.rsplit_once("__").unwrap_or((stem, "0_0_0"));

        // Skip any file with an invalid name or version
        let name = match name_str.parse::<Name>() {
            Ok(name) => name,
            Err(err) => {
                tracing::debug!(file_path = %manifest_path, error = ?err, "Skipping file with invalid name");
                continue;
            }
        };

        let version = match version_str.replace('_', ".").parse::<Version>() {
            Ok(version) => version,
            Err(err) => {
                tracing::debug!(file_path = %manifest_path, error = ?err, "Skipping file with invalid version");
                continue;
            }
        };

        set.insert((name, version, manifest_path));
    }

    set
}

/// Errors specific to manifest storage operations
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Failed to serialize manifest to JSON
    #[error("Failed to serialize manifest to JSON: {0}")]
    ManifestSerialization(#[source] serde_json::Error),

    /// Failed to store manifest in object store
    #[error("Failed to store manifest in object store: {0}")]
    ManifestStorage(#[source] object_store::Error),
}

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
        Err(err) => Err(err.into()),
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

/// JSON parsing error when deserializing manifest content
#[derive(Debug, thiserror::Error)]
#[error("JSON parsing error: {0}")]
pub struct ManifestParseError(#[source] pub serde_json::Error);
