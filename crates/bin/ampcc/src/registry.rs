//! Registry client for the public Amp dataset registry.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::AuthStorage;

/// Errors that can occur when interacting with the registry.
#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Registry returned error: {status} - {message}")]
    Api { status: u16, message: String },
}

/// A dataset from the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryDataset {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub latest_version: Option<LatestVersion>,
    #[serde(default)]
    pub description: Option<String>,
}

/// Latest version info embedded in dataset response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatestVersion {
    #[serde(default)]
    pub version_tag: Option<String>,
}

/// A version of a dataset from the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryVersion {
    #[serde(default)]
    pub version_tag: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub changelog: Option<String>,
}

/// Response from listing datasets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetListResponse {
    pub datasets: Vec<RegistryDataset>,
    #[serde(default)]
    pub has_next_page: bool,
    #[serde(default)]
    pub total_count: Option<u32>,
}

/// Response from searching datasets.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSearchResponse {
    pub datasets: Vec<RegistryDataset>,
    #[serde(default)]
    pub has_next_page: bool,
}

/// Client for interacting with the Amp registry API.
#[derive(Clone)]
pub struct RegistryClient {
    http: reqwest::Client,
    base_url: String,
    auth_token: Option<String>,
}

impl RegistryClient {
    /// Create a new registry client with auto-detected authentication.
    ///
    /// Loads auth token from:
    /// 1. AMP_AUTH_TOKEN environment variable (highest priority)
    /// 2. Auth storage file (~/.amp/cache/amp_cli_auth)
    pub fn new(base_url: String) -> Self {
        // Load auth from AMP_AUTH_TOKEN env var. If not present, load from storage
        let token =
            Self::load_auth_from_env().or_else(|| AuthStorage::load().map(|a| a.access_token));
        Self::with_token(base_url, token)
    }

    /// Create a new registry client with an explicit auth token.
    ///
    /// Use this when the app's auth state changes (login, logout, refresh).
    pub fn with_token(base_url: String, token: Option<String>) -> Self {
        let http = reqwest::Client::new();

        Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            auth_token: token,
        }
    }

    /// Load auth token from AMP_AUTH_TOKEN environment variable.
    ///
    /// This is used as a fallback for CI/automation scenarios.
    fn load_auth_from_env() -> Option<String> {
        std::env::var("AMP_AUTH_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())
    }

    /// Build a request with optional auth header.
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.http.request(method, &url);

        if let Some(token) = &self.auth_token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }

        req.header("Accept", "application/json")
            .header("Content-Type", "application/json")
    }

    /// List datasets with pagination.
    pub async fn list_datasets(&self, page: u32) -> Result<DatasetListResponse, RegistryError> {
        let path = format!("/api/v1/datasets?limit=50&page={}", page);
        let response = self.request(reqwest::Method::GET, &path).send().await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response.text().await.unwrap_or_default();
            return Err(RegistryError::Api { status, message });
        }

        response
            .json::<DatasetListResponse>()
            .await
            .map_err(RegistryError::from)
    }

    /// Search datasets by keyword.
    #[allow(dead_code)]
    pub async fn search_datasets(
        &self,
        query: &str,
    ) -> Result<Vec<RegistryDataset>, RegistryError> {
        let path = format!(
            "/api/v1/datasets/search?search={}&limit=50",
            urlencoding::encode(query)
        );
        let response = self.request(reqwest::Method::GET, &path).send().await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response.text().await.unwrap_or_default();
            return Err(RegistryError::Api { status, message });
        }

        let resp: DatasetSearchResponse = response.json().await?;
        Ok(resp.datasets)
    }

    /// Get all versions of a dataset.
    pub async fn get_versions(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Vec<RegistryVersion>, RegistryError> {
        let path = format!("/api/v1/datasets/{}/{}/versions", namespace, name);
        let response = self.request(reqwest::Method::GET, &path).send().await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response.text().await.unwrap_or_default();
            return Err(RegistryError::Api { status, message });
        }

        response
            .json::<Vec<RegistryVersion>>()
            .await
            .map_err(RegistryError::from)
    }

    /// Get the manifest for a specific dataset version.
    pub async fn get_manifest(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<serde_json::Value, RegistryError> {
        let path = format!(
            "/api/v1/datasets/{}/{}/versions/{}/manifest",
            namespace, name, version
        );
        let response = self.request(reqwest::Method::GET, &path).send().await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response.text().await.unwrap_or_default();
            return Err(RegistryError::Api { status, message });
        }

        response
            .json::<serde_json::Value>()
            .await
            .map_err(RegistryError::from)
    }

    /// Test the connection by listing a single dataset.
    #[allow(dead_code)]
    pub async fn test_connection(&self) -> Result<(), RegistryError> {
        let path = "/api/v1/datasets?limit=1&page=1";
        let response = self.request(reqwest::Method::GET, path).send().await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response.text().await.unwrap_or_default();
            return Err(RegistryError::Api { status, message });
        }

        Ok(())
    }
}
