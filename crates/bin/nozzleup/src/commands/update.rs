use anyhow::{Context, Result};

use crate::{
    config::Config,
    github::GitHubClient,
    platform::{Architecture, Platform},
};

pub async fn run() -> Result<()> {
    println!("nozzleup: Updating nozzleup...");

    let config = Config::new()?;
    let github = GitHubClient::new(&config)?;

    // Get the latest version
    let latest_version = github.get_latest_version().await?;
    println!("nozzleup: Latest version: {}", latest_version);

    // Detect platform and architecture
    let platform = Platform::detect()?;
    let arch = Architecture::detect()?;

    // Download the nozzleup binary
    let artifact_name = format!("nozzleup-{}-{}", platform.as_str(), arch.as_str());

    println!("nozzleup: Downloading {} ...", artifact_name);

    let binary_data = github
        .download_release_asset(&latest_version, &artifact_name)
        .await
        .context("Failed to download nozzleup binary")?;

    // Get the current executable path
    let current_exe = std::env::current_exe().context("Failed to get current executable path")?;

    // Write to a temporary file first
    let temp_path = current_exe.with_extension("tmp");
    fs_err::write(&temp_path, &binary_data).context("Failed to write temporary file")?;

    // Make it executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs_err::metadata(&temp_path)
            .context("Failed to get temp file metadata")?
            .permissions();
        perms.set_mode(0o755);
        fs_err::set_permissions(&temp_path, perms)
            .context("Failed to set executable permissions")?;
    }

    // Replace the current executable
    fs_err::rename(&temp_path, &current_exe).context("Failed to replace executable")?;

    println!("nozzleup: Updated successfully to {}", latest_version);

    Ok(())
}
