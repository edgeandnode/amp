//! ampctl fixture for registering dataset manifests and provider configurations.
//!
//! This fixture provides a convenient interface for registering dataset manifests
//! and provider configurations with the Admin API in test environments. It uses
//! the ampctl admin API client for direct programmatic access.

use common::BoxError;
use datasets_common::reference::Reference;
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
    ) -> Result<(), BoxError> {
        let fqn = dataset_ref.as_fqn();
        let revision = dataset_ref.revision().as_version();

        self.client
            .datasets()
            .register(fqn, revision, manifest_content)
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
    pub async fn register_provider(
        &self,
        provider_name: &str,
        provider_toml: &str,
    ) -> Result<(), BoxError> {
        // Parse TOML content
        let toml_value: toml::Value = toml::from_str(provider_toml)
            .map_err(|err| format!("Failed to parse provider TOML: {}", err))?;

        // Validate that the provider is a TOML table
        if !toml_value.is_table() {
            return Err("Provider TOML must be a table at the root level".into());
        }

        // Convert TOML to JSON for API request
        let json_value: serde_json::Value = serde_json::to_value(&toml_value)
            .map_err(|err| format!("Failed to convert TOML to JSON: {}", err))?;

        // Register with the client
        self.client
            .providers()
            .register(provider_name, &json_value)
            .await
            .map_err(Into::into)
    }
}
