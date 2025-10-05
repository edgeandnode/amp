use anyhow::{Context, Result};
use serde::Deserialize;

use crate::config::Config;

#[derive(Debug, Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<Asset>,
}

#[derive(Debug, Deserialize)]
struct Asset {
    id: u64,
    name: String,
    browser_download_url: String,
}

pub struct GitHubClient {
    client: reqwest::Client,
    repo: String,
    token: Option<String>,
}

impl GitHubClient {
    pub fn new(config: &Config) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::USER_AGENT,
            reqwest::header::HeaderValue::from_static("nozzleup"),
        );

        if let Some(token) = &config.github_token {
            let auth_value = format!("Bearer {}", token);
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&auth_value)
                    .context("Invalid GitHub token")?,
            );
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            repo: config.repo.clone(),
            token: config.github_token.clone(),
        })
    }

    /// Get the latest release version
    pub async fn get_latest_version(&self) -> Result<String> {
        let url = format!("https://api.github.com/repos/{}/releases/latest", self.repo);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch latest release")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to fetch latest release: HTTP {} - {}", status, body);
        }

        let release: Release = response
            .json()
            .await
            .context("Failed to parse release response")?;

        Ok(release.tag_name)
    }

    /// Get a specific release
    async fn get_release(&self, version: &str) -> Result<Release> {
        let url = format!(
            "https://api.github.com/repos/{}/releases/tags/{}",
            self.repo, version
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch release")?;

        if !response.status().is_success() {
            let status = response.status();
            anyhow::bail!("Release {} not found: HTTP {}", version, status);
        }

        let release: Release = response
            .json()
            .await
            .context("Failed to parse release response")?;

        Ok(release)
    }

    /// Download a release asset by name
    pub async fn download_release_asset(&self, version: &str, asset_name: &str) -> Result<Vec<u8>> {
        let release = self.get_release(version).await?;

        // Find the asset
        let asset = release
            .assets
            .iter()
            .find(|a| a.name == asset_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Asset '{}' not found in release {}. Available assets: {}",
                    asset_name,
                    version,
                    release
                        .assets
                        .iter()
                        .map(|a| a.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;

        // For private repositories, we need to use the API to download
        if self.token.is_some() {
            self.download_asset_via_api(asset.id).await
        } else {
            // For public repositories, use direct download URL
            self.download_asset_direct(&asset.browser_download_url)
                .await
        }
    }

    /// Download asset via GitHub API (for private repos)
    async fn download_asset_via_api(&self, asset_id: u64) -> Result<Vec<u8>> {
        let url = format!(
            "https://api.github.com/repos/{}/releases/assets/{}",
            self.repo, asset_id
        );

        let response = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT, "application/octet-stream")
            .send()
            .await
            .context("Failed to download asset")?;

        if !response.status().is_success() {
            let status = response.status();
            anyhow::bail!("Failed to download asset: HTTP {}", status);
        }

        let bytes = response
            .bytes()
            .await
            .context("Failed to read asset data")?;

        Ok(bytes.to_vec())
    }

    /// Download asset directly (for public repos)
    async fn download_asset_direct(&self, url: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("Failed to download asset")?;

        if !response.status().is_success() {
            let status = response.status();
            anyhow::bail!("Failed to download asset: HTTP {}", status);
        }

        let bytes = response
            .bytes()
            .await
            .context("Failed to read asset data")?;

        Ok(bytes.to_vec())
    }

    /// Download SHA256 checksum for an asset
    pub async fn download_checksum(&self, version: &str, asset_name: &str) -> Result<String> {
        let checksum_name = format!("{}.sha256", asset_name);

        match self.download_release_asset(version, &checksum_name).await {
            Ok(data) => {
                let checksum =
                    String::from_utf8(data).context("Checksum file is not valid UTF-8")?;
                // Extract just the hash (first part before any whitespace)
                let hash = checksum
                    .split_whitespace()
                    .next()
                    .context("Invalid checksum format")?;
                Ok(hash.to_string())
            }
            Err(e) => {
                // Checksum file might not exist
                Err(e)
            }
        }
    }
}
