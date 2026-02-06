//! ampctl fixture for registering dataset manifests and provider configurations.
//!
//! This fixture provides a convenient interface for registering dataset manifests
//! and provider configurations with the Admin API in test environments. It uses
//! the ampctl admin API client for direct programmatic access.

use ampctl::client::datasets::NodeSelector;
use anyhow::{Result, anyhow};
use datasets_common::{hash::Hash, reference::Reference};
use dump::EndBlock;
use serde_json::value::RawValue;
use url::Url;

/// ampctl fixture for registering dataset manifests and provider configurations.
///
/// This fixture provides methods for registering dataset manifests and provider
/// configurations with the Admin API. It uses the ampctl admin API client to
/// make HTTP requests directly.
#[derive(Clone, Debug)]
pub struct Ampctl {
    client: ampctl::client::Client,
}

impl Ampctl {
    /// Create a new ampctl fixture from the admin API URL.
    ///
    /// Takes the admin API URL from a running Amp controller and creates an
    /// admin API client for manifest and provider registration requests.
    ///
    /// # Panics
    ///
    /// Panics if the provided URL is invalid. This is a programming error in tests.
    pub fn new(admin_api_url: impl Into<String>) -> Self {
        let url_str = admin_api_url.into();
        let admin_url = Url::parse(&url_str)
            .unwrap_or_else(|err| panic!("Invalid admin URL '{}': {}", url_str, err));

        Self {
            client: ampctl::client::Client::new(admin_url),
        }
    }

    /// Get the admin URL this ampctl is configured to use.
    pub fn admin_url(&self) -> &Url {
        self.client.base_url()
    }

    /// Register a dataset manifest with the Admin API.
    ///
    /// Takes a dataset reference (namespace/name@version) and the manifest content
    /// as a JSON string, then POSTs it to the `/datasets` endpoint using the
    /// admin API client.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The network request fails
    /// - The API returns an error response (400/409/500)
    /// - The API returns an unexpected status code
    pub async fn register_manifest(
        &self,
        dataset_ref: &Reference,
        manifest_content: &str,
    ) -> Result<()> {
        let fqn = dataset_ref.as_fqn();
        let revision = dataset_ref.revision().as_version();

        let manifest_json: Box<RawValue> = serde_json::from_str(manifest_content)
            .map_err(|err| anyhow!("Failed to parse manifest JSON: {}", err))?;

        self.client
            .datasets()
            .register(fqn, revision, manifest_json)
            .await
            .map_err(Into::into)
    }

    /// Register a provider configuration with the Admin API.
    ///
    /// Takes a provider name and the provider configuration as a TOML string,
    /// converts it to JSON, and POSTs it to the `/providers` endpoint using the
    /// admin API client.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The TOML content is invalid
    /// - The TOML root is not a table/object
    /// - The network request fails
    /// - The API returns an error response (400/409/500)
    /// - The API returns an unexpected status code
    pub async fn register_provider(&self, provider_name: &str, provider_toml: &str) -> Result<()> {
        // Parse TOML content
        let toml_value: toml::Value = toml::from_str(provider_toml)
            .map_err(|err| anyhow!("Failed to parse provider TOML: {}", err))?;

        // Validate that the provider is a TOML table
        if !toml_value.is_table() {
            return Err(anyhow!("Provider TOML must be a table at the root level"));
        }

        // Convert TOML to JSON for API request
        let json_value: serde_json::Value = serde_json::to_value(&toml_value)
            .map_err(|err| anyhow!("Failed to convert TOML to JSON: {}", err))?;

        // Register with the client
        self.client
            .providers()
            .register(provider_name, &json_value)
            .await
            .map_err(Into::into)
    }

    /// Get the manifest hash for the latest version of a dataset.
    ///
    /// Takes a dataset reference (namespace/name) and retrieves the manifest hash
    /// for the first (latest) version from the versions list.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The network request fails
    /// - The API returns an error response
    /// - No versions are found for the dataset
    /// - The manifest hash cannot be parsed
    pub async fn get_latest_manifest_hash(&self, dataset_ref: &Reference) -> Result<Hash> {
        let fqn = dataset_ref.as_fqn();

        let versions_response = self
            .client
            .datasets()
            .list_versions(fqn)
            .await
            .map_err(|err| anyhow!("Failed to list dataset versions: {}", err))?;

        versions_response
            .versions
            .first()
            .map(|v| v.manifest_hash.clone())
            .ok_or(anyhow!("No versions found for dataset"))
    }

    /// Restore a dataset's physical tables from object storage.
    ///
    /// Takes a dataset reference (namespace/name@version) and POSTs to the
    /// `/datasets/{namespace}/{name}/versions/{version}/restore` endpoint to
    /// re-index physical table metadata from storage into the metadata database.
    ///
    /// Returns information about the restored tables including their names,
    /// location IDs, and storage URLs.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The network request fails
    /// - The API returns an error response (400/404/500)
    /// - The dataset or revision is not found
    /// - Table data is not found in object storage
    pub async fn restore_dataset(
        &self,
        dataset_ref: &Reference,
    ) -> Result<Vec<ampctl::client::datasets::RestoredTableInfo>> {
        self.client
            .datasets()
            .restore(dataset_ref)
            .await
            .map(|response| response.tables)
            .map_err(Into::into)
    }

    /// Get the jobs client for job status monitoring.
    ///
    /// This provides access to the jobs API for checking job status, stopping jobs,
    /// and other job management operations.
    pub fn jobs(&self) -> ampctl::client::jobs::JobsClient<'_> {
        self.client.jobs()
    }

    /// Deploy a dataset by scheduling a dump job.
    ///
    /// Takes a dataset reference string (namespace/name@version) and optional parameters,
    /// then schedules a dump job to extract and process the dataset.
    pub async fn dataset_deploy(
        &self,
        dataset_ref: &str,
        end_block: Option<u64>,
        parallelism: Option<u16>,
        worker_id: Option<NodeSelector>,
    ) -> Result<worker::job::JobId> {
        let reference: Reference = dataset_ref.parse().map_err(|err| {
            anyhow!(
                "Failed to parse dataset reference '{}': {}",
                dataset_ref,
                err
            )
        })?;

        let end_block_param = end_block.map(EndBlock::Absolute);

        self.client
            .datasets()
            .deploy(
                &reference,
                end_block_param,
                parallelism.unwrap_or(1),
                worker_id,
            )
            .await
            .map_err(Into::into)
    }
}
