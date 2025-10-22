//! ampctl fixture for registering dataset manifests and provider configurations.
//!
//! This fixture provides a convenient interface for registering dataset manifests
//! and provider configurations with the Admin API in test environments. It wraps
//! the registration logic from the ampctl CLI tool.

use common::BoxError;
use datasets_common::reference::Reference;
use url::Url;

/// ampctl fixture for registering dataset manifests and provider configurations.
///
/// This fixture provides methods for registering dataset manifests and provider
/// configurations with the Admin API. It automatically handles the HTTP request
/// formatting and error handling.
#[derive(Clone, Debug)]
pub struct Ampctl {
    admin_url: Url,
}

impl Ampctl {
    /// Create a new ampctl fixture from the admin API URL.
    ///
    /// Takes the admin API URL from a running Amp controller and uses it
    /// for manifest and provider registration requests.
    ///
    /// # Panics
    ///
    /// Panics if the provided URL is invalid. This is a programming error in tests.
    pub fn new(admin_api_url: impl Into<String>) -> Self {
        let url_str = admin_api_url.into();
        let admin_url = Url::parse(&url_str)
            .unwrap_or_else(|err| panic!("Invalid admin URL '{}': {}", url_str, err));

        Self { admin_url }
    }

    /// Get the admin URL this ampctl is configured to use.
    pub fn admin_url(&self) -> &Url {
        &self.admin_url
    }

    /// Register a dataset manifest with the Admin API.
    ///
    /// Takes a dataset reference (namespace/name@version) and the manifest content
    /// as a JSON string, then POSTs it to the `/datasets` endpoint using the
    /// ampctl registration logic.
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
        ampctl::cmd::reg_manifest::register_manifest(&self.admin_url, dataset_ref, manifest_content)
            .await
            .map_err(|err| err.into())
    }

    /// Register a provider configuration with the Admin API.
    ///
    /// Takes a provider name and the provider configuration as a TOML string,
    /// then POSTs it to the `/providers` endpoint using the ampctl registration logic.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The TOML content is invalid
    /// - The network request fails
    /// - The API returns an error response (400/409/500)
    /// - The API returns an unexpected status code
    pub async fn register_provider(
        &self,
        provider_name: &str,
        provider_toml: &str,
    ) -> Result<(), BoxError> {
        ampctl::cmd::reg_provider::register_provider(&self.admin_url, provider_name, provider_toml)
            .await
            .map_err(|err| err.into())
    }
}
