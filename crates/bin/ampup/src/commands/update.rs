use anyhow::{Context, Result};
use semver::Version;

use crate::{github::GitHubClient, ui, updater::Updater};

pub async fn run(repo: String, github_token: Option<String>) -> Result<()> {
    ui::info!("Checking for updates");

    let github = GitHubClient::new(repo, github_token)?;
    let updater = Updater::new(github);

    let current_version = updater.get_current_version();
    let latest_version = updater.get_latest_version().await?;

    ui::info!(
        "Current version: {}, Latest version: {}",
        ui::version(&current_version),
        ui::version(&latest_version)
    );

    // Normalize versions for comparison (e.g., "v0.1.0-123-gabcd1234" -> "0.1.0")
    let current_normalized = current_version
        .split('-')
        .next()
        .unwrap_or(&current_version)
        .strip_prefix('v')
        .unwrap_or(&current_version);
    let latest_normalized = latest_version
        .split('-')
        .next()
        .unwrap_or(&latest_version)
        .strip_prefix('v')
        .unwrap_or(&latest_version);

    // Parse versions for proper semver comparison
    let current_semver =
        Version::parse(current_normalized).context("Failed to parse current version")?;
    let latest_semver =
        Version::parse(latest_normalized).context("Failed to parse latest version")?;

    if latest_semver > current_semver {
        ui::info!("Updating to {}", ui::version(&latest_version));
        updater.update_self(&latest_version).await?;
    } else {
        ui::success!("No updates available");
    }

    Ok(())
}
