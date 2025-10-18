use std::{collections::BTreeSet, sync::Arc};

use common::store::ObjectStoreExt;
use datasets_common::{
    hash::{Hash, hash},
    manifest::Manifest as CommonManifest,
    name::Name,
    namespace::Namespace,
    version::Version,
};
use datasets_derived::Manifest as DerivedDatasetManifest;
use eth_beacon_datasets::Manifest as EthBeaconManifest;
use evm_rpc_datasets::Manifest as EvmRpcManifest;
use firehose_datasets::dataset::Manifest as FirehoseManifest;
use futures::StreamExt as _;
use metadata_db::MetadataDb;
use object_store::{ObjectStore, path::Path as ObjectStorePath};

use crate::DatasetKind;

/// Manages dataset manifest configurations with object store operations
///
/// ## Manifest Storage Formats
///
/// This store supports two manifest storage formats:
///
/// ### Hash-Based Format (Primary)
/// - **Storage**: Manifests are stored as `{hash}` (no file extension)
/// - **Content Addressing**: Filename is the SHA-256 hash of the manifest JSON content
/// - **Deduplication**: Identical manifests share the same file
/// - **Retrieval**: Use [`get`](Self::get) with the manifest hash
/// - **Creation**: Use [`store`](Self::store) to write new manifests
///
/// ### Name-Based Format (Fallback for Initialization)
/// - **Purpose**: Fallback mechanism to preload existing manifests during initialization
/// - **Storage**: Manifests stored with `.json` extension using name/version patterns
/// - **Supported Patterns**:
///   - `{namespace}__{name}__{version}.json` - Full specification
///   - `{name}__{version}.json` - Defaults to namespace `_`
///   - `{name}.json` - Defaults to namespace `_` and version `0.0.0`
/// - **Processing**: Only during `init()` - files are scanned, hashed, and registered in metadata DB
/// - **Not for Runtime**: Name-based files are NOT used during normal operations
///
/// ## Preloading Fallback Mechanism
///
/// The initialization process provides a migration path from name-based to hash-based storage:
///
/// 1. **Discovery**: `init()` scans object store for `.json` files (name-based format)
/// 2. **Hash Computation**: For each file, content is fetched and SHA-256 hash computed
/// 3. **Metadata Registration**: Dataset is registered in metadata DB with:
///    - Extracted namespace, name, and version from filename
///    - Computed manifest hash
///    - Original file path (for reference)
/// 4. **Deduplication**: If dataset already exists in metadata DB, it's skipped
///
/// **Important**: Hash-based manifests (files without `.json` extension) are automatically
/// filtered out during initialization since they should already be registered when created.
///
/// ## Initialization Requirements
///
/// **IMPORTANT**: The module-level [`init`] function MUST be called explicitly before using this store.
/// Failure to initialize will result in:
/// - Name-based manifests not being preloaded into the metadata database
/// - Potential data inconsistency between object store and metadata database
///
/// The initialization:
/// - Scans the object store for name-based manifest files (`.json` extension)
/// - Computes hashes from file contents
/// - Registers manifests in metadata database with namespace/name/version tags
/// - Is safe to call multiple times (idempotent via module-level `OnceCell`)
/// - Runs asynchronously and should be awaited at application startup
/// - Uses module-level state, so initialization is process-wide (not per-instance)
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

    /// Get a dataset manifest by hash from the object store
    ///
    /// Returns the manifest content if found, or None if not found.
    /// This operation directly queries the underlying store without caching.
    ///
    /// ## Note
    ///
    /// This method expects hash-based manifest filenames without extension (e.g., `{hash}`).
    /// Name-based manifests (fallback format with `.json` extension) are not supported by this method.
    pub async fn get(&self, hash: &Hash) -> Result<Option<ManifestContent>, GetError> {
        let manifest_path = ObjectStorePath::from(hash.to_string());
        fetch_manifest_content(&self.store, manifest_path)
            .await
            .map_err(GetError::ObjectStoreFetch)
    }

    /// Store a new dataset manifest in the underlying store
    ///
    /// Returns the path where the manifest was stored.
    /// Note: This function only stores the manifest file, it does not register
    /// the dataset in the metadata database. The caller is responsible for that.
    ///
    /// The manifest is stored with hash-based filename format (no extension): `{hash}`
    ///
    /// The caller must provide the manifest in JSON string format. This string should be
    /// the canonical serialized form of the manifest (already validated and serialized).
    pub async fn store(
        &self,
        hash: &Hash,
        manifest_str: String,
    ) -> Result<ObjectStorePath, StoreError> {
        let manifest_path = ObjectStorePath::from(hash.to_string());

        self.store
            .put(&manifest_path, manifest_str.into())
            .await
            .map_err(StoreError)?;

        Ok(manifest_path)
    }
}

/// Initialize the manifests store by loading existing manifests into the metadata database
///
/// This function ensures that all manifests present in the object store are registered in the
/// provided metadata database. It's idempotent and will only register manifests that don't
/// already exist. The initialization runs only once per process using a module-level OnceCell.
///
/// ## Important
///
/// This function must be called explicitly at application startup before using the store.
/// The OnceCell ensures the initialization logic runs only once even if called multiple times.
// TODO: Move logic to the admin cli (ampctl) once available
pub async fn init<S>(store: &DatasetManifestsStore<S>, metadata_db: &MetadataDb)
where
    S: ObjectStore + Clone,
{
    let manifests = list(&store.store).await;
    register_manifests_batch(&store.store, metadata_db, manifests).await;
}

/// Register manifests in batch from name-based files to hash-based storage
///
/// Takes an iterator of manifest tuples (namespace, name, version, path) from name-based
/// `.json` files and migrates them to hash-based storage while registering in metadata database.
///
/// Processing is done in two passes to handle dependencies:
/// 1. **First pass**: Register raw datasets (Firehose, EvmRpc, EthBeacon) which have no dependencies
/// 2. **Second pass**: Register derived datasets which may depend on raw datasets from the first pass
///
/// For each manifest:
/// 1. Checks if it already exists in the metadata database (skips if so)
/// 2. Fetches the manifest content from the name-based path
/// 3. Validates and parses based on dataset kind
/// 4. Computes the manifest hash from canonical JSON
/// 5. Stores the manifest with hash-based filename (`{hash}`)
/// 6. Registers in metadata database with the hash-based path
// TODO: Move logic to the admin cli (ampctl) once available
async fn register_manifests_batch<S, I>(store: &S, metadata_db: &MetadataDb, manifests: I)
where
    S: ObjectStore,
    I: IntoIterator<Item = (Namespace, Name, Version, ManifestPath)>,
{
    tracing::info!(
        "Initializing ManifestsStore: loading manifests from store into metadata database"
    );

    // Collect all manifests and separate into raw and derived
    let all_manifests: Vec<_> = manifests.into_iter().collect();
    let mut raw_manifests = Vec::new();
    let mut derived_manifests = Vec::new();

    // First, categorize manifests by checking their kind
    for (namespace, name, version, path) in &all_manifests {
        // Fetch manifest content to determine kind
        let manifest_content = match fetch_manifest_content(store, path.clone().into()).await {
            Ok(Some(content)) => content,
            Ok(None) | Err(_) => continue,
        };

        let manifest_str = manifest_content.into_json_string();
        let common_manifest = match serde_json::from_str::<CommonManifest>(&manifest_str) {
            Ok(manifest) => manifest,
            Err(_) => continue,
        };

        let dataset_kind = match common_manifest.kind.parse::<DatasetKind>() {
            Ok(kind) => kind,
            Err(_) => continue,
        };

        match dataset_kind {
            DatasetKind::Derived => {
                derived_manifests.push((
                    namespace.clone(),
                    name.clone(),
                    version.clone(),
                    path.clone(),
                ));
            }
            DatasetKind::Firehose | DatasetKind::EvmRpc | DatasetKind::EthBeacon => {
                raw_manifests.push((
                    namespace.clone(),
                    name.clone(),
                    version.clone(),
                    path.clone(),
                ));
            }
        }
    }

    // Process raw datasets first, then derived datasets
    let mut total_count = 0;
    let mut registered_count = 0;
    let mut error_count = 0;

    tracing::debug!(
        raw_datasets = raw_manifests.len(),
        derived_datasets = derived_manifests.len(),
        "Processing manifests in two passes: raw datasets first, then derived datasets"
    );

    for (namespace, name, version, path) in raw_manifests
        .into_iter()
        .chain(derived_manifests.into_iter())
    {
        total_count += 1;

        // Check if the dataset already exists in the metadata database
        let dataset_exists = match metadata_db
            .dataset_exists(&namespace, &name, &version)
            .await
        {
            Ok(exists) => exists,
            Err(err) => {
                error_count += 1;
                tracing::warn!(
                    dataset_namespace = %namespace,
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
                dataset_namespace = %namespace,
                dataset_name = %name,
                dataset_version = %version,
                "Dataset already exists in metadata database, skipping"
            );
            continue;
        }

        // Fetch manifest content to compute hash
        let manifest_content = match fetch_manifest_content(store, path.clone().into()).await {
            Ok(Some(content)) => content,
            Ok(None) => {
                error_count += 1;
                tracing::warn!(
                    dataset_namespace = %namespace,
                    dataset_name = %name,
                    dataset_version = %version,
                    manifest_path = %path,
                    "Manifest file not found in object store"
                );
                continue;
            }
            Err(err) => {
                error_count += 1;
                tracing::warn!(
                    dataset_namespace = %namespace,
                    dataset_name = %name,
                    dataset_version = %version,
                    manifest_path = %path,
                    error = %err,
                    "Failed to fetch manifest content from object store"
                );
                continue;
            }
        };

        // Parse as CommonManifest first to determine the dataset kind
        let manifest_str = manifest_content.into_json_string();
        let common_manifest = match serde_json::from_str::<CommonManifest>(&manifest_str) {
            Ok(manifest) => manifest,
            Err(err) => {
                error_count += 1;
                tracing::warn!(
                    dataset_namespace = %namespace,
                    dataset_name = %name,
                    dataset_version = %version,
                    manifest_path = %path,
                    error = ?err,
                    "Failed to parse manifest as valid CommonManifest structure"
                );
                continue;
            }
        };

        // Parse the dataset kind from the manifest
        let dataset_kind = match common_manifest.kind.parse::<DatasetKind>() {
            Ok(kind) => kind,
            Err(err) => {
                error_count += 1;
                tracing::warn!(
                    dataset_namespace = %namespace,
                    dataset_name = %name,
                    dataset_version = %version,
                    manifest_path = %path,
                    kind = %common_manifest.kind,
                    error = ?err,
                    "Unsupported dataset kind"
                );
                continue;
            }
        };

        // Validate and re-serialize based on the specific dataset kind
        let (manifest_hash, canonical_str) = match dataset_kind {
            DatasetKind::Derived => {
                match parse_validate_and_canonicalize_derived_dataset_manifest(&manifest_str) {
                    Ok(result) => result,
                    Err(ParseDerivedManifestError::Deserialization(err)) => {
                        error_count += 1;
                        tracing::warn!(
                            dataset_namespace = %namespace,
                            dataset_name = %name,
                            dataset_version = %version,
                            manifest_path = %path,
                            kind = %dataset_kind,
                            error = ?err,
                            "Failed to parse manifest as DerivedDatasetManifest"
                        );
                        continue;
                    }
                    Err(ParseDerivedManifestError::DependencyValidation(err)) => {
                        error_count += 1;
                        tracing::warn!(
                            dataset_namespace = %namespace,
                            dataset_name = %name,
                            dataset_version = %version,
                            manifest_path = %path,
                            kind = %dataset_kind,
                            error = ?err,
                            "Manifest dependency validation failed"
                        );
                        continue;
                    }
                    Err(ParseDerivedManifestError::Serialization(err)) => {
                        error_count += 1;
                        tracing::warn!(
                            dataset_namespace = %namespace,
                            dataset_name = %name,
                            dataset_version = %version,
                            manifest_path = %path,
                            kind = %dataset_kind,
                            error = ?err,
                            "Failed to re-serialize manifest to canonical JSON"
                        );
                        continue;
                    }
                }
            }
            DatasetKind::EvmRpc => {
                match parse_and_canonicalize_raw_dataset_manifest::<EvmRpcManifest>(&manifest_str) {
                    Ok(result) => result,
                    Err(err) => {
                        error_count += 1;
                        tracing::warn!(
                            dataset_namespace = %namespace,
                            dataset_name = %name,
                            dataset_version = %version,
                            manifest_path = %path,
                            kind = %dataset_kind,
                            error = ?err,
                            "Failed to parse and canonicalize EvmRpc manifest"
                        );
                        continue;
                    }
                }
            }
            DatasetKind::Firehose => {
                match parse_and_canonicalize_raw_dataset_manifest::<FirehoseManifest>(&manifest_str)
                {
                    Ok(result) => result,
                    Err(err) => {
                        error_count += 1;
                        tracing::warn!(
                            dataset_namespace = %namespace,
                            dataset_name = %name,
                            dataset_version = %version,
                            manifest_path = %path,
                            kind = %dataset_kind,
                            error = ?err,
                            "Failed to parse and canonicalize Firehose manifest"
                        );
                        continue;
                    }
                }
            }
            DatasetKind::EthBeacon => {
                match parse_and_canonicalize_raw_dataset_manifest::<EthBeaconManifest>(
                    &manifest_str,
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        error_count += 1;
                        tracing::warn!(
                            dataset_namespace = %namespace,
                            dataset_name = %name,
                            dataset_version = %version,
                            manifest_path = %path,
                            kind = %dataset_kind,
                            error = ?err,
                            "Failed to parse and canonicalize EthBeacon manifest"
                        );
                        continue;
                    }
                }
            }
        };

        // Store manifest with hash-based filename in object store
        let hash_based_path = ObjectStorePath::from(manifest_hash.to_string());
        if let Err(err) = store.put(&hash_based_path, canonical_str.into()).await {
            error_count += 1;
            tracing::warn!(
                dataset_name = %name,
                dataset_version = %version,
                manifest_hash = %manifest_hash,
                error = ?err,
                "Failed to store manifest with hash-based filename"
            );
            continue;
        }

        // Register the manifest in the metadata database using hash-based path
        if let Err(err) = metadata_db
            .register_manifest(&namespace, &name, &manifest_hash, hash_based_path.as_ref())
            .await
        {
            error_count += 1;
            tracing::warn!(
                dataset_name = %name,
                dataset_version = %version,
                manifest_path = %path,
                error = %err,
                "Failed to register manifest in metadata database"
            );
            continue;
        }

        // Tag the manifest with the version
        match metadata_db
            .register_tag(&namespace, &name, &version, &manifest_hash)
            .await
        {
            Ok(()) => {
                registered_count += 1;
                tracing::debug!(
                    dataset_namespace = %namespace,
                    dataset_name = %name,
                    dataset_version = %version,
                    manifest_path = %path,
                    "Successfully registered manifest and tagged version in metadata database"
                );
            }
            Err(err) => {
                error_count += 1;
                tracing::warn!(
                    dataset_namespace = %namespace,
                    dataset_name = %name,
                    dataset_version = %version,
                    manifest_path = %path,
                    error = %err,
                    "Failed to tag version in metadata database"
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

/// List dataset manifests using name-based fallback formats for initialization tagging
///
/// Parses name-based fallback formats into ([`Namespace`], [`Name`], [`Version`]) tuples:
/// - `{namespace}__{name}__{version}.json`
/// - `{name}__{version}.json` (defaults: namespace `_`)
/// - `{name}.json` (defaults: namespace `_`, version `0.0.0`)
///
/// These formats are used as a fallback mechanism during initialization to load
/// manifests and tag them with versions in the metadata database.
// TODO: Move to the admin cli (ampctl) once available
async fn list<S>(store: &S) -> BTreeSet<(Namespace, Name, Version, ManifestPath)>
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
        let path = match ManifestPath::try_from(manifest_path_str.clone()) {
            Ok(path) => path,
            Err(err) => {
                tracing::debug!(path = %manifest_path_str, error = ?err, "Skipping file with unsupported format");
                continue;
            }
        };

        // Skip any path with no file name
        let Some(file_name) = path.as_ref().filename() else {
            tracing::debug!(path = %path, "Skipping path with no filename");
            continue;
        };

        let stem = file_name.trim_end_matches(".json");

        // Parse name-based fallback formats by splitting on "__"
        let parts: Vec<&str> = stem.split("__").collect();
        let (namespace_str, name_str, version_str) = match parts.as_slice() {
            // {namespace}__{name}__{version}.json
            [namespace, name, version] => (*namespace, *name, *version),
            // {name}__{version}.json (default namespace: _)
            [name, version] => ("_", *name, *version),
            // {name}.json (default namespace: _, version: 0.0.0)
            [name] => ("_", *name, "0_0_0"),
            // Invalid format
            _ => {
                tracing::debug!(file_path = %path, "Skipping file with invalid format (too many parts)");
                continue;
            }
        };

        // Parse namespace
        let namespace = match namespace_str.parse::<Namespace>() {
            Ok(namespace) => namespace,
            Err(err) => {
                tracing::debug!(file_path = %path, error = ?err, "Skipping file with invalid namespace");
                continue;
            }
        };

        // Skip any file with an invalid name or version
        let name = match name_str.parse::<Name>() {
            Ok(name) => name,
            Err(err) => {
                tracing::debug!(file_path = %path, error = ?err, "Skipping file with invalid name");
                continue;
            }
        };

        let version = match version_str.replace('_', ".").parse::<Version>() {
            Ok(version) => version,
            Err(err) => {
                tracing::debug!(file_path = %path, error = ?err, "Skipping file with invalid version");
                continue;
            }
        };

        set.insert((namespace, name, version, path));
    }

    set
}

/// Error when storing manifest in object store
#[derive(Debug, thiserror::Error)]
#[error("Failed to store manifest in object store: {0}")]
pub struct StoreError(#[source] pub object_store::Error);

/// Errors specific to manifest retrieval operations
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// Failed to fetch manifest from object store
    #[error("Failed to fetch manifest from object store: {0}")]
    ObjectStoreFetch(common::store::StoreError),
}

/// Fetches manifest content from the object store
///
/// Retrieves the content of a manifest file (JSON format) from the provided
/// object store. The function gracefully handles missing files by returning `None`.
async fn fetch_manifest_content<S>(
    store: &S,
    path: ObjectStorePath,
) -> Result<Option<ManifestContent>, common::store::StoreError>
where
    S: ObjectStore,
{
    match store.get_string(path).await {
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

/// Parse and re-serialize a raw dataset manifest to canonical JSON format
///
/// This function handles the common pattern for raw datasets (EvmRpc, Firehose, EthBeacon):
/// 1. Deserialize from JSON string
/// 2. Re-serialize to canonical JSON
/// 3. Compute hash of canonical representation
///
/// Returns tuple of (manifest_hash, canonical_json_string) on success
fn parse_and_canonicalize_raw_dataset_manifest<T>(
    manifest_str: impl AsRef<str>,
) -> Result<(Hash, String), serde_json::Error>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let manifest: T = serde_json::from_str(manifest_str.as_ref())?;
    let canonical_str = serde_json::to_string(&manifest)?;
    Ok((hash(&canonical_str), canonical_str))
}

/// Parse, validate, and re-serialize a derived dataset manifest to canonical JSON format
///
/// This function handles derived datasets which require dependency validation:
/// 1. Deserialize from JSON string
/// 2. Validate dependencies using `manifest.validate_dependencies()`
/// 3. Re-serialize to canonical JSON
/// 4. Compute hash of canonical representation
///
/// Returns tuple of (manifest_hash, canonical_json_string) on success
fn parse_validate_and_canonicalize_derived_dataset_manifest(
    manifest_str: impl AsRef<str>,
) -> Result<(Hash, String), ParseDerivedManifestError> {
    let manifest: DerivedDatasetManifest = serde_json::from_str(manifest_str.as_ref())
        .map_err(ParseDerivedManifestError::Deserialization)?;

    manifest
        .validate_dependencies()
        .map_err(ParseDerivedManifestError::DependencyValidation)?;

    let canonical_str =
        serde_json::to_string(&manifest).map_err(ParseDerivedManifestError::Serialization)?;

    Ok((hash(&canonical_str), canonical_str))
}

/// Error type for derived dataset manifest parsing
#[derive(Debug)]
enum ParseDerivedManifestError {
    Deserialization(serde_json::Error),
    DependencyValidation(datasets_derived::manifest::DependencyValidationError),
    Serialization(serde_json::Error),
}
