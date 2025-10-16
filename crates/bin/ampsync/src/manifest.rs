use std::time::Duration;

use admin_api::handlers::datasets::get_versions::DatasetVersionsResponse;
use datasets_common::{name::Name, version_tag::VersionTag};
use datasets_derived::Manifest;
use lazy_static::lazy_static;
use thiserror::Error;

/// Errors that can occur when fetching dataset manifests from admin-api.
#[derive(Error, Debug)]
pub enum ManifestError {
    #[error("Dataset '{dataset}' version '{version}' not found in admin-api")]
    DatasetNotFound { dataset: String, version: String },

    #[error("No versions available for dataset '{dataset}' in admin-api")]
    NoVersionsAvailable { dataset: String },

    #[error("Failed to connect to admin-api at {url}: {message}")]
    NetworkError { url: String, message: String },

    #[error("HTTP {status} from admin-api {url}: {message}")]
    HttpError {
        status: u16,
        url: String,
        message: String,
    },

    #[error("Invalid manifest")]
    InvalidManifest(#[source] anyhow::Error),
}

impl ManifestError {
    /// Returns true if this error is retryable (transient/recoverable).
    ///
    /// Retryable errors include:
    /// - Dataset not found (maybe published soon)
    /// - No versions available (maybe published soon)
    /// - Network errors (maybe transient)
    /// - 5xx HTTP errors (server-side issues, may recover)
    pub fn is_retryable(&self) -> bool {
        match self {
            ManifestError::DatasetNotFound { .. }
            | ManifestError::NoVersionsAvailable { .. }
            | ManifestError::NetworkError { .. }
            | ManifestError::InvalidManifest { .. } => true,
            ManifestError::HttpError { status, .. } => *status >= 500,
        }
    }
}

/// Maximum number of dataset versions to fetch from admin-api when resolving version
const ADMIN_API_VERSION_LIMIT: usize = 1000;

lazy_static! {
    /// Shared HTTP client for admin-api requests with optimized connection pooling.
    ///
    /// Creating a new client for each request is expensive as it spawns new connection pools,
    /// DNS resolvers, and TLS session caches. This shared client reuses connections efficiently.
    static ref ADMIN_API_CLIENT: reqwest::Client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(10)
        .tcp_keepalive(Duration::from_secs(60))
        .build()
        .expect("Failed to create HTTP client");
}

/// Fetch ONLY the latest version without fetching the full schema.
///
/// This is used by version polling to efficiently check for new versions
/// without the overhead of fetching the schema. Only calls the versions endpoint.
///
/// # Arguments
/// * `admin_api_addr` - Base URL of the admin-api service
/// * `name` - Dataset name
///
/// # Returns
/// * `Ok(Version)` - The latest (first) version from the versions list
/// * `Err(ManifestError)` - If no versions available or HTTP request fails
pub async fn fetch_latest_version(
    admin_api_addr: &str,
    name: &Name,
) -> Result<VersionTag, ManifestError> {
    resolve_qualified_version(admin_api_addr, name, None).await
}

/// Resolve a simple version to a fully qualified version, or get the latest version.
///
/// This function queries the admin-api to get all versions for a dataset.
/// - If `version` is Some: Find the first version that starts with the given version prefix (e.g., "0.2.0" → "0.2.0-LTcyNjgzMjc1NA")
/// - If `version` is None: Return the first (latest) version from the list
///
/// # Arguments
/// * `admin_api_addr` - Base URL of the admin-api service
/// * `name` - Dataset name
/// * `version` - Optional simple version from the config (e.g., "0.2.0"). If None, returns latest.
///
/// # Returns
/// * `Ok(Version)` - The fully qualified version
/// * `Err(ManifestError)` - If the version cannot be found or HTTP request fails
async fn resolve_qualified_version(
    admin_api_addr: &str,
    name: &Name,
    version: Option<&VersionTag>,
) -> Result<VersionTag, ManifestError> {
    let url = format!(
        "{}/datasets/{}/versions?limit={}",
        admin_api_addr, name, ADMIN_API_VERSION_LIMIT
    );

    let versions_resp =
        ADMIN_API_CLIENT
            .get(&url)
            .send()
            .await
            .map_err(|e| ManifestError::NetworkError {
                url: url.clone(),
                message: e.to_string(),
            })?;

    // Check for HTTP errors
    let status = versions_resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(ManifestError::NoVersionsAvailable {
            dataset: name.to_string(),
        });
    }
    if !status.is_success() {
        let body = versions_resp.text().await.unwrap_or_default();
        return Err(ManifestError::HttpError {
            status: status.as_u16(),
            url: url.clone(),
            message: body,
        });
    }

    let versions_data: DatasetVersionsResponse =
        versions_resp
            .json()
            .await
            .map_err(|e| ManifestError::NetworkError {
                url: url.clone(),
                message: e.to_string(),
            })?;

    // Check if any versions exist
    if versions_data.versions.is_empty() {
        return Err(ManifestError::NoVersionsAvailable {
            dataset: name.to_string(),
        });
    }

    // Resolve version: either match prefix or use first (latest) version
    let qualified_version = match version {
        Some(v) => {
            // Find the first version that matches our config version prefix
            // The config has a simple version like "0.2.0", but we need the fully qualified
            // version like "0.2.0-LTcyNjgzMjc1NA"
            let version_prefix = v.to_string();
            versions_data
                .versions
                .iter()
                .find(|ver| ver.to_string().starts_with(&version_prefix))
                .ok_or_else(|| ManifestError::DatasetNotFound {
                    dataset: name.to_string(),
                    version: version_prefix.clone(),
                })?
                .clone()
        }
        None => {
            // No specific version requested - use first (latest) version
            versions_data
                .versions
                .first()
                .expect("versions list is not empty (checked above)")
                .clone()
        }
    };

    tracing::info!(
        dataset = %name,
        requested_version = ?version,
        qualified_version = %qualified_version,
        "version_resolved"
    );

    Ok(qualified_version)
}

/// Fetch manifest with indefinite polling for initial startup.
///
/// This function polls the admin-api until the dataset becomes available.
/// Used during initial startup when the dataset might not be published yet.
///
/// # Arguments
/// * `admin_api_addr` - Base URL of the admin-api service
/// * `name` - Dataset name
/// * `version` - Optional dataset version. If None, uses latest version.
///
/// # Returns
/// * `Ok(Manifest)` - Successfully fetched manifest
/// * `Err(ManifestError)` - Non-recoverable errors (permanent failures)
///
/// # Behavior
/// - On retryable errors: Retry with exponential backoff (indefinitely)
/// - On non-retryable errors: Fail immediately
/// - Logs helpful messages to guide users to run `nozzle dump`
pub async fn fetch_manifest_with_startup_poll(
    admin_api_addr: &str,
    name: &Name,
    version: Option<&VersionTag>,
) -> Result<Manifest, ManifestError> {
    let mut first_error_logged = false;
    let mut attempt = 0u32;
    let max_backoff_secs = 30u64;

    loop {
        match fetch_manifest(admin_api_addr, name, version).await {
            Ok(manifest) => {
                if attempt > 0 {
                    tracing::info!(
                        "Successfully fetched manifest after {} attempts",
                        attempt + 1
                    );
                }
                return Ok(manifest);
            }
            Err(e) if e.is_retryable() => {
                if !first_error_logged {
                    tracing::warn!(
                        error = %e,
                        "dataset_not_found_waiting_for_publish"
                    );
                    tracing::warn!(
                        "Have you run 'nozzle dump --dataset <name>' to publish the dataset?"
                    );
                    tracing::info!("waiting_for_dataset");
                    first_error_logged = true;
                }

                // Calculate backoff with saturation to avoid overflow
                let backoff_secs = if attempt < 5 {
                    2u64.pow(attempt)
                } else {
                    max_backoff_secs
                }
                .min(max_backoff_secs);

                if attempt > 0 && attempt.is_multiple_of(5) {
                    tracing::info!(
                        attempt = attempt + 1,
                        retry_delay_secs = backoff_secs,
                        "still_waiting_for_dataset"
                    );
                }

                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                attempt += 1;
            }
            Err(e) => {
                // Non-retryable error
                return Err(e);
            }
        }
    }
}

/// Fetch and construct a dataset Manifest from admin-api schema.
///
/// This function performs the following steps:
/// 1. Resolves the version (if provided) to a fully qualified version
/// 2. Fetches the raw manifest from the admin-api /datasets/{name}/versions/{version}/manifest endpoint
///
/// # Arguments
/// * `admin_api_addr` - Base URL of the admin-api service (e.g., "http://localhost:1610")
/// * `name` - Dataset name
/// * `version` - Optional dataset version. If None, uses first (latest) version from list.
///
/// # Returns
/// * `Ok(Manifest)` - A complete manifest with schema and generated SQL
/// * `Err(ManifestError)` - Various errors:
///   - Invalid dataset name or version
///   - HTTP request failure to admin-api
///   - Dataset not found
///
/// # Example
/// ```ignore
/// let name: Name = "my_dataset".parse()?;
/// let version: Version = "0.1.0".parse()?;
/// let manifest = fetch_manifest(
///     "http://localhost:1610",
///     &name,
///     Some(&version)
/// ).await?;
/// ```
pub async fn fetch_manifest(
    admin_api_addr: &str,
    name: &Name,
    version: Option<&VersionTag>,
) -> Result<Manifest, ManifestError> {
    // Resolve version (handles both explicit version and "latest" via None)
    let qualified_version = resolve_qualified_version(admin_api_addr, name, version).await?;

    tracing::info!(
        dataset = %name,
        version = %qualified_version,
        admin_api_addr = %admin_api_addr,
        "fetching_schema"
    );

    let manifest_url = format!(
        "{}/datasets/{}/versions/{}/manifest",
        admin_api_addr, name, qualified_version
    );
    let manifest_resp = ADMIN_API_CLIENT
        .get(&manifest_url)
        .send()
        .await
        .map_err(|e| ManifestError::NetworkError {
            url: manifest_url.clone(),
            message: e.to_string(),
        })?;
    let status = manifest_resp.status();
    if !status.is_success() {
        let body = manifest_resp.text().await.unwrap_or_default();
        return Err(ManifestError::HttpError {
            status: status.as_u16(),
            url: manifest_url.clone(),
            message: body,
        });
    }
    manifest_resp
        .json::<Manifest>()
        .await
        .map_err(|e| ManifestError::InvalidManifest(e.into()))
}
