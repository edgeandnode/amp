//! Release Manifest Generator Binary
//!
//! Generates release manifest from GitHub release assets.

use std::collections::BTreeMap;

// Constants
const ASSET_NAME_PATTERN: &str = "<component>-<os>-<arch>";

/// Generate release manifest from GitHub release assets
#[derive(Debug, clap::Parser)]
#[command(name = "ampup-gen-release-manifest")]
#[command(about = "Generate release manifest from GitHub release assets", long_about = None)]
struct Args {
    /// GitHub repository in format "owner/name" (e.g., "edgeandnode/amp")
    repository: github::Repo,

    /// Git tag (e.g., "v1.0.0") or "latest" for the latest release (default: "latest")
    #[arg(default_value = "latest")]
    tag: String,

    /// GitHub token for authentication (reads from GITHUB_TOKEN env var)
    #[arg(env = "GITHUB_TOKEN", hide = true)]
    github_token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = <Args as clap::Parser>::parse();

    eprintln!("════════════════════════════════════════════════════════════════");
    eprintln!("🚀 Release Manifest Generator");
    eprintln!("════════════════════════════════════════════════════════════════");
    eprintln!(
        "Repository: {}/{}",
        args.repository.owner(),
        args.repository.name()
    );
    eprintln!("Tag: {}", args.tag);
    eprintln!("Output: stdout\n");

    eprintln!("📥 Fetching release from GitHub API...\n");

    // Fetch release from GitHub (latest or specific tag)
    let release = if args.tag == "latest" {
        github::fetch_latest(&args.repository, args.github_token.as_deref())
            .await
            .map_err(Error::FetchRelease)?
    } else {
        github::fetch_release(&args.repository, &args.tag, args.github_token.as_deref())
            .await
            .map_err(Error::FetchRelease)?
    };

    // Extract version from tag (remove 'v' prefix)
    let version = release
        .tag_name
        .strip_prefix('v')
        .unwrap_or(&release.tag_name)
        .to_string();

    eprintln!(
        "\n📋 Generating manifest from {} assets...\n",
        release.assets.len()
    );

    // Generate manifest
    let manifest = generate_manifest(&release, &version, &args)?;

    // Write manifest to stdout
    eprintln!("\n💾 Writing manifest to stdout...");
    let manifest_json = serde_json::to_string_pretty(&manifest).map_err(Error::JsonSerialize)?;
    println!("{}", manifest_json);

    eprintln!("\n════════════════════════════════════════════════════════════════");
    eprintln!("✅ SUCCESS: Generated manifest to stdout");
    eprintln!("════════════════════════════════════════════════════════════════\n");

    // Print summary to stderr
    eprintln!("Manifest summary:");
    eprintln!("  Version: {}", manifest.version);
    eprintln!("  Components: {}", manifest.components.len());
    for (component, data) in &manifest.components {
        eprintln!("    - {}: {} target(s)", component, data.targets.len());
    }
    eprintln!();

    Ok(())
}

/// Errors that can occur during manifest generation
#[derive(Debug, thiserror::Error)]
enum Error {
    /// Failed to fetch release from GitHub API
    ///
    /// This error wraps all failures that can occur when fetching release metadata
    /// from the GitHub API, including network errors, authentication failures, and
    /// response parsing errors.
    #[error("Failed to fetch release from GitHub API")]
    FetchRelease(#[source] github::FetchError),

    /// Asset missing required digest field
    ///
    /// This occurs when a release asset does not have the required SHA-256 digest field.
    /// All assets must include a digest for security and integrity verification.
    #[error("Asset '{asset_name}' missing digest field")]
    MissingDigest { asset_name: String },

    /// No binary components found in release
    ///
    /// This occurs when the release contains no assets matching the expected binary
    /// naming pattern. The release must contain at least one binary asset.
    #[error(
        "No binary components found in release! Expected assets matching pattern: {ASSET_NAME_PATTERN}"
    )]
    NoComponents,

    /// Failed to serialize manifest to JSON
    ///
    /// This occurs when the generated manifest cannot be serialized to JSON,
    /// typically due to invalid UTF-8 or other serialization constraints.
    #[error("Failed to serialize manifest to JSON")]
    JsonSerialize(#[source] serde_json::Error),
}

/// Generate manifest from GitHub release assets
///
/// # Errors
/// Returns error if:
/// - No valid binary assets found in release
/// - Asset missing required digest field
fn generate_manifest(
    release: &github::Release,
    version: &str,
    args: &Args,
) -> Result<ampup::manifest::Manifest, Error> {
    use ampup::manifest::{AssetName, Component, MANIFEST_VERSION, Manifest, Repository, Target};

    let mut component_map: BTreeMap<String, Component> = BTreeMap::new();

    // Process each asset in the release
    for asset in &release.assets {
        let parsed: AssetName = match asset.name.parse() {
            Ok(name) => name,
            Err(_) => {
                eprintln!("⚠️  Skipping non-binary asset: {}", asset.name);
                continue;
            }
        };

        eprintln!(
            "✓ Discovered: {} → component={}, target={}",
            asset.name,
            parsed.component(),
            parsed.target()
        );

        // Validate digest field is present
        let hash = asset.digest.as_ref().ok_or_else(|| Error::MissingDigest {
            asset_name: asset.name.clone(),
        })?;

        // Initialize component if first time seeing it
        let component = component_map
            .entry(parsed.component().to_string())
            .or_insert_with(|| Component {
                version: version.to_string(),
                targets: BTreeMap::new(),
            });

        // Check if notarized (macOS targets)
        let notarized = parsed.is_macos();

        // Add target to component
        component.targets.insert(
            parsed.target(),
            Target {
                url: asset.browser_download_url.clone(),
                hash: hash.clone(),
                size: asset.size,
                notarized,
            },
        );

        eprintln!(
            "  → Added target {} ({} bytes, notarized: {})",
            parsed.target(),
            asset.size,
            notarized
        );
    }

    // Validate we discovered at least one component
    if component_map.is_empty() {
        return Err(Error::NoComponents);
    }

    eprintln!("\n✅ Discovered {} component(s)", component_map.len());
    for (component, data) in &component_map {
        eprintln!("   - {}: {} target(s)", component, data.targets.len());
    }

    // Build final manifest
    let manifest = Manifest {
        manifest_version: MANIFEST_VERSION.to_string(),
        date: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        version: version.to_string(),
        repository: Repository {
            owner: args.repository.owner().to_string(),
            name: args.repository.name().to_string(),
            url: format!("https://github.com/{}", args.repository),
        },
        components: component_map,
    };

    Ok(manifest)
}

mod github {
    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};

    /// Build GitHub API URL for fetching latest release.
    ///
    /// GET `/repos/{owner}/{name}/releases/latest`
    pub fn api_latest_release(repo: &Repo) -> String {
        format!("https://api.github.com/repos/{}/releases/latest", repo)
    }

    /// Build GitHub API URL for fetching release by tag.
    ///
    /// GET `/repos/{owner}/{name}/releases/tags/{tag}`
    pub fn api_release_by_tag(repo: &Repo, tag: &str) -> String {
        format!(
            "https://api.github.com/repos/{}/releases/tags/{}",
            repo, tag
        )
    }

    /// Fetch latest release metadata from GitHub API
    pub async fn fetch_latest(
        repo: &Repo,
        github_token: Option<&str>,
    ) -> Result<Release, FetchError> {
        fetch_release_from_url(api_latest_release(repo), github_token).await
    }

    /// Fetch release metadata from GitHub API by tag
    pub async fn fetch_release(
        repo: &Repo,
        tag: &str,
        github_token: Option<&str>,
    ) -> Result<Release, FetchError> {
        fetch_release_from_url(api_release_by_tag(repo, tag), github_token).await
    }

    /// Internal helper to fetch release metadata from a GitHub API URL
    async fn fetch_release_from_url(
        url: String,
        github_token: Option<&str>,
    ) -> Result<Release, FetchError> {
        eprintln!("  URL: {}", url);

        let client = reqwest::Client::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("ampup-gen-release-manifest"),
        );

        if let Some(token) = github_token {
            eprintln!("  Authentication: Using GITHUB_TOKEN");
            let auth_value = format!("{} {}", "Bearer", token);
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&auth_value).map_err(FetchError::InvalidTokenFormat)?,
            );
        } else {
            eprintln!("  Authentication: None (unauthenticated)");
        }

        let response = client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(FetchError::HttpRequest)?;

        if !response.status().is_success() {
            return Err(FetchError::ApiRequestFailed {
                status: response.status().as_u16(),
                body: response.text().await.unwrap_or_default(),
            });
        }

        let release: Release = response.json().await.map_err(FetchError::JsonParse)?;

        eprintln!("  ✅ Release: {}", release.name);
        eprintln!("  ✅ Assets: {}", release.assets.len());

        Ok(release)
    }

    /// Errors that can occur when fetching release from GitHub API
    #[derive(Debug, thiserror::Error)]
    pub enum FetchError {
        /// Failed to create HTTP request headers with provided GitHub token
        ///
        /// This occurs when the GitHub token contains invalid characters that cannot
        /// be used in HTTP headers.
        #[error("Invalid GitHub token format")]
        InvalidTokenFormat(#[source] reqwest::header::InvalidHeaderValue),

        /// Failed to send HTTP request to GitHub API
        ///
        /// This occurs when the network request fails due to connectivity issues,
        /// DNS resolution failures, or other network-level problems.
        ///
        /// Possible causes:
        /// - No internet connectivity
        /// - DNS resolution failure for api.github.com
        /// - Network timeout
        /// - TLS/SSL handshake failure
        #[error("Failed to send HTTP request to GitHub API")]
        HttpRequest(#[source] reqwest::Error),

        /// GitHub API returned non-success status code
        ///
        /// This occurs when the GitHub API request completes but returns an error status.
        ///
        /// Common status codes:
        /// - 401: Invalid or missing authentication token
        /// - 404: Repository or release tag not found
        /// - 403: Rate limit exceeded or insufficient permissions
        /// - 500+: GitHub server errors
        #[error("GitHub API request failed with status {status}: {body}")]
        ApiRequestFailed { status: u16, body: String },

        /// Failed to parse GitHub API JSON response
        ///
        /// This occurs when the response body is not valid JSON or doesn't match
        /// the expected Release schema.
        #[error("Failed to parse GitHub API response as JSON")]
        JsonParse(#[source] reqwest::Error),
    }

    /// Repository in format `owner/name`
    #[derive(Debug, Clone)]
    pub struct Repo(String);

    impl Repo {
        /// Returns the repository owner
        pub fn owner(&self) -> &str {
            self.0.split('/').next().unwrap()
        }

        /// Returns the repository name
        pub fn name(&self) -> &str {
            self.0.split('/').nth(1).unwrap()
        }
    }

    impl std::fmt::Display for Repo {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::str::FromStr for Repo {
        type Err = RepoParseError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let parts: Vec<&str> = s.split('/').collect();

            let [owner, name] = parts.as_slice() else {
                return Err(RepoParseError::InvalidFormat {
                    input: s.to_string(),
                });
            };

            if owner.is_empty() {
                return Err(RepoParseError::EmptyOwner {
                    input: s.to_string(),
                });
            }
            if name.is_empty() {
                return Err(RepoParseError::EmptyName {
                    input: s.to_string(),
                });
            }

            Ok(Repo(s.to_string()))
        }
    }

    /// Errors that can occur when parsing a repository string
    #[derive(Debug, thiserror::Error)]
    pub enum RepoParseError {
        /// Repository string does not contain exactly one '/' separator
        #[error("Invalid repository format '{input}'. Expected format: owner/name")]
        InvalidFormat { input: String },

        /// Owner component is empty
        #[error("Empty owner in repository '{input}'. Expected format: owner/name")]
        EmptyOwner { input: String },

        /// Repository name component is empty
        #[error("Empty repository name in '{input}'. Expected format: owner/name")]
        EmptyName { input: String },
    }

    /// Release information from GitHub API
    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Release {
        /// Release name (display title)
        pub name: String,
        /// Git tag name (canonical version identifier)
        pub tag_name: String,
        /// List of release assets
        pub assets: Vec<Asset>,
    }

    /// Release asset metadata from GitHub API
    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Asset {
        /// Asset filename
        pub name: String,
        /// Browser download URL
        pub browser_download_url: String,
        /// File size in bytes
        pub size: u64,
        /// SHA-256 digest in format "sha256:abc123..."
        ///
        /// Note: GitHub API provides this field automatically.
        /// If missing, asset cannot be included in manifest.
        #[serde(default)]
        pub digest: Option<String>,
    }
}
