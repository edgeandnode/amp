use std::time::Duration;

use admin_api::handlers::datasets::get::DatasetInfo;
use datasets_common::{name::Name, namespace::Namespace, version::Version};
use datasets_derived::Manifest;
use lazy_static::lazy_static;

/// Errors that can occur when fetching dataset manifests from admin-api.
#[derive(Debug, thiserror::Error)]
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
/// without the overhead of fetching the schema. Uses the "latest" version endpoint.
///
/// Uses the default namespace "_".
pub async fn fetch_latest_version(
    admin_api_addr: &str,
    namespace: &Namespace,
    name: &Name,
) -> Result<Version, ManifestError> {
    let url = format!(
        "{}/datasets/{}/{}/versions/latest",
        admin_api_addr, namespace, name
    );

    let response =
        ADMIN_API_CLIENT
            .get(&url)
            .send()
            .await
            .map_err(|e| ManifestError::NetworkError {
                url: url.clone(),
                message: e.to_string(),
            })?;

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(ManifestError::NoVersionsAvailable {
            dataset: name.to_string(),
        });
    }
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ManifestError::HttpError {
            status: status.as_u16(),
            url: url.clone(),
            message: body,
        });
    }

    let dataset_info: DatasetInfo =
        response
            .json()
            .await
            .map_err(|e| ManifestError::NetworkError {
                url: url.clone(),
                message: e.to_string(),
            })?;

    // The revision field contains the resolved version
    dataset_info.revision.to_string().parse().map_err(|e| {
        ManifestError::InvalidManifest(anyhow::anyhow!("Invalid version format: {}", e))
    })
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
    namespace: &Namespace,
    name: &Name,
    version: Option<&Version>,
) -> Result<Manifest, ManifestError> {
    let mut first_error_logged = false;
    let mut attempt = 0u32;
    let max_backoff_secs = 30u64;

    loop {
        match fetch_manifest(admin_api_addr, namespace, name, version).await {
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
/// Fetches the manifest from the admin-api endpoint. If version is None, uses "latest".
///
/// Uses the default namespace "_".
pub async fn fetch_manifest(
    admin_api_addr: &str,
    namespace: &Namespace,
    name: &Name,
    version: Option<&Version>,
) -> Result<Manifest, ManifestError> {
    let version_str = version
        .map(|v| v.to_string())
        .unwrap_or_else(|| "latest".to_string());

    tracing::info!(
        namespace = %namespace,
        dataset = %name,
        version = %version_str,
        admin_api_addr = %admin_api_addr,
        "fetching_manifest"
    );

    let manifest_url = format!(
        "{}/datasets/{}/{}/versions/{}/manifest",
        admin_api_addr, namespace, name, version_str
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
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(ManifestError::DatasetNotFound {
            dataset: name.to_string(),
            version: version_str.clone(),
        });
    }
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
