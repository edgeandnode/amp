use anyhow::{Context, Result};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Deserialize;

const AMPUP_API_URL: &str = "https://ampup.sh/api";
const GITHUB_API_URL: &str = "https://api.github.com";

#[derive(Debug)]
pub enum GitHubError {
    ReleaseNotFound {
        repo: String,
        has_token: bool,
        url: String,
        is_latest: bool,
    },
    AuthFailed {
        status_code: u16,
        repo: String,
        url: String,
    },
    AssetNotFound {
        repo: String,
        asset_name: String,
        version: String,
        available_assets: Vec<String>,
    },
    DownloadFailed {
        repo: String,
        asset_name: String,
        status_code: u16,
        url: String,
    },
    HttpError {
        repo: String,
        status_code: u16,
        url: String,
        body: String,
    },
}

impl std::fmt::Display for GitHubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReleaseNotFound {
                repo,
                has_token,
                url,
                is_latest,
            } => {
                if *is_latest {
                    writeln!(f, "Failed to fetch latest release")?;
                } else {
                    writeln!(f, "Failed to fetch release")?;
                }
                writeln!(f, "  Repository: {}", repo)?;
                writeln!(f, "  URL: {}", url)?;
                writeln!(f, "  Status: 404 Not Found")?;
                writeln!(f)?;
                if *has_token {
                    writeln!(
                        f,
                        "  The repository may not exist, or no releases have been published yet."
                    )?;
                    if !*is_latest {
                        writeln!(f, "  The specified version/tag may not exist.")?;
                    }
                } else {
                    writeln!(f, "  The repository is private or requires authentication.")?;
                    writeln!(f, "  Try: export GITHUB_TOKEN=$(gh auth token)")?;
                }
            }
            Self::AuthFailed {
                status_code,
                repo,
                url,
            } => {
                writeln!(f, "Authentication failed")?;
                writeln!(f, "  Repository: {}", repo)?;
                writeln!(f, "  URL: {}", url)?;
                writeln!(f, "  Status: HTTP {}", status_code)?;
                writeln!(f)?;
                writeln!(f, "  Your GITHUB_TOKEN may be invalid or expired.")?;
                if *status_code == 403 {
                    writeln!(
                        f,
                        "  For private repositories, ensure your token has 'repo' scope."
                    )?;
                }
                writeln!(f, "  Try: export GITHUB_TOKEN=$(gh auth token)")?;
            }
            Self::AssetNotFound {
                repo,
                asset_name,
                version,
                available_assets,
            } => {
                writeln!(f, "Release asset not found")?;
                writeln!(f, "  Repository: {}", repo)?;
                writeln!(f, "  Asset: {}", asset_name)?;
                writeln!(f, "  Version: {}", version)?;
                writeln!(f)?;
                if available_assets.is_empty() {
                    writeln!(f, "  No assets available in this release.")?;
                } else {
                    writeln!(f, "  Available assets:")?;
                    for asset in available_assets {
                        writeln!(f, "    - {}", asset)?;
                    }
                }
            }
            Self::DownloadFailed {
                repo,
                asset_name,
                status_code,
                url,
            } => {
                writeln!(f, "Failed to download release asset")?;
                writeln!(f, "  Repository: {}", repo)?;
                writeln!(f, "  Asset: {}", asset_name)?;
                writeln!(f, "  URL: {}", url)?;
                writeln!(f, "  Status: HTTP {}", status_code)?;
                writeln!(f)?;
                if *status_code == 401 || *status_code == 403 {
                    writeln!(f, "  Authentication or permission issue.")?;
                    writeln!(f, "  Try: export GITHUB_TOKEN=$(gh auth token)")?;
                } else if *status_code == 404 {
                    writeln!(f, "  The asset may have been removed or is not accessible.")?;
                } else {
                    writeln!(f, "  Network or server error. Please try again.")?;
                }
            }
            Self::HttpError {
                repo,
                status_code,
                url,
                body,
            } => {
                writeln!(f, "Request failed")?;
                writeln!(f, "  Repository: {}", repo)?;
                writeln!(f, "  URL: {}", url)?;
                writeln!(f, "  Status: HTTP {}", status_code)?;
                if !body.is_empty() {
                    writeln!(f, "  Response: {}", body)?;
                }
            }
        }
        Ok(())
    }
}

impl std::error::Error for GitHubError {}

#[derive(Debug, Deserialize)]
struct Release {
    #[serde(rename = "tag_name")]
    tag: String,
    assets: Vec<Asset>,
}

#[derive(Debug, Deserialize)]
struct Asset {
    id: u64,
    name: String,
    #[serde(rename = "browser_download_url")]
    url: String,
}

pub struct GitHubClient {
    client: reqwest::Client,
    repo: String,
    token: Option<String>,
    /// Base URL for API requests (either custom API or GitHub API)
    api: String,
}

impl GitHubClient {
    pub fn new(repo: String, github_token: Option<String>) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::USER_AGENT,
            reqwest::header::HeaderValue::from_static("ampup"),
        );

        if let Some(token) = &github_token {
            let auth_value = format!("Bearer {}", token);
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&auth_value)
                    .context("Invalid access token")?,
            );
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .context("Failed to create request client")?;

        // Use custom endpoints for edgeandnode/amp and otherwise leverages the github api
        let api = if repo == "edgeandnode/amp" && github_token.is_none() {
            AMPUP_API_URL.to_string()
        } else {
            format!("{}/repos/{}/releases", GITHUB_API_URL, repo)
        };

        Ok(Self {
            client,
            repo,
            token: github_token,
            api,
        })
    }

    /// Get the latest release version
    pub async fn get_latest_version(&self) -> Result<String> {
        let release = self.get_latest_release().await?;
        Ok(release.tag)
    }

    /// Get the latest release
    async fn get_latest_release(&self) -> Result<Release> {
        self.get_release("latest").await
    }

    /// Get a tagged release
    async fn get_tagged_release(&self, version: &str) -> Result<Release> {
        self.get_release(&format!("tags/{}", version)).await
    }

    /// Fetch release from GitHub API
    async fn get_release(&self, path: &str) -> Result<Release> {
        let url = format!("{}/{}", self.api, path);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch release")?;

        if !response.status().is_success() {
            let status = response.status();
            match status {
                reqwest::StatusCode::NOT_FOUND => {
                    return Err(GitHubError::ReleaseNotFound {
                        repo: self.repo.clone(),
                        has_token: self.token.is_some(),
                        url: url.clone(),
                        is_latest: path == "latest",
                    }
                    .into());
                }
                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                    return Err(GitHubError::AuthFailed {
                        status_code: status.as_u16(),
                        repo: self.repo.clone(),
                        url: url.clone(),
                    }
                    .into());
                }
                _ => {
                    let body = response.text().await.unwrap_or_default();
                    return Err(GitHubError::HttpError {
                        repo: self.repo.clone(),
                        status_code: status.as_u16(),
                        url: url.clone(),
                        body,
                    }
                    .into());
                }
            }
        }

        let release: Release = response
            .json()
            .await
            .context("Failed to parse release response")?;

        Ok(release)
    }

    /// Download a release asset by name
    pub async fn download_release_asset(&self, version: &str, asset_name: &str) -> Result<Vec<u8>> {
        let release = self.get_tagged_release(version).await?;

        // Find the asset
        let asset = release
            .assets
            .iter()
            .find(|a| a.name == asset_name)
            .ok_or_else(|| GitHubError::AssetNotFound {
                repo: self.repo.clone(),
                asset_name: asset_name.to_string(),
                version: version.to_string(),
                available_assets: release.assets.iter().map(|a| a.name.clone()).collect(),
            })?;

        if self.token.is_some() {
            // For private repositories, we need to use the API to download
            self.download_asset_via_api(asset.id, asset_name).await
        } else {
            // For public repositories, use direct download URL
            self.download_asset_direct(&asset.url, asset_name).await
        }
    }

    /// Download asset via GitHub API (for private repos)
    async fn download_asset_via_api(&self, asset_id: u64, asset_name: &str) -> Result<Vec<u8>> {
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

        self.download_with_progress(response, &url, asset_name)
            .await
    }

    /// Download asset directly (for public repos)
    async fn download_asset_direct(&self, url: &str, asset_name: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("Failed to download asset")?;

        self.download_with_progress(response, url, asset_name).await
    }

    /// Download with progress bar from a response
    async fn download_with_progress(
        &self,
        response: reqwest::Response,
        url: &str,
        asset_name: &str,
    ) -> Result<Vec<u8>> {
        if !response.status().is_success() {
            let status = response.status();
            return Err(GitHubError::DownloadFailed {
                repo: self.repo.clone(),
                asset_name: asset_name.to_string(),
                status_code: status.as_u16(),
                url: url.to_string(),
            }
            .into());
        }

        // Get content length for progress bar
        let total_size = response.content_length();

        // Setup progress bar
        let pb = if let Some(size) = total_size {
            let pb = ProgressBar::new(size);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
                    )
                    .context("Invalid progress bar template")?
                    .progress_chars("#>-"),
            );
            pb.set_message(format!("{} Downloading", console::style("→").cyan()));
            pb
        } else {
            let pb = ProgressBar::new_spinner();
            pb.set_message(format!(
                "{} Downloading (size unknown)",
                console::style("→").cyan()
            ));
            pb
        };

        // Stream and collect chunks
        let mut downloaded: u64 = 0;
        let mut buffer = Vec::new();
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("Error while downloading file")?;
            buffer.extend_from_slice(&chunk);
            downloaded += chunk.len() as u64;
            pb.set_position(downloaded);
        }

        pb.finish_with_message(format!("{} Downloaded", console::style("✓").green().bold()));

        Ok(buffer)
    }
}
