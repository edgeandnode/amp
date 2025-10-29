use anyhow::Result;

use crate::{
    config::Config,
    github::GitHubClient,
    install::Installer,
    platform::{Architecture, Platform},
    ui,
    version_manager::VersionManager,
};

pub async fn run(
    install_dir: Option<std::path::PathBuf>,
    repo: String,
    github_token: Option<String>,
    version: Option<String>,
    arch_override: Option<String>,
    platform_override: Option<String>,
) -> Result<()> {
    let config = Config::new(install_dir)?;
    let github = GitHubClient::new(repo, github_token)?;
    let version_manager = VersionManager::new(config);

    // Determine version to install
    let version = match version {
        Some(v) => v,
        None => {
            ui::info!("Fetching latest version");
            github.get_latest_version().await?
        }
    };

    // Check if this version is already installed
    if version_manager.is_installed(&version) {
        ui::info!("Version {} is already installed", ui::version(&version));

        // Check if it's the current version
        let current_version = version_manager.get_current()?;
        if current_version.as_deref() == Some(&version) {
            ui::success!("Already using version {}", ui::version(&version));
            return Ok(());
        }

        // Switch to this version
        ui::info!("Switching to version {}", ui::version(&version));
        crate::commands::use_version::switch_to_version(&version_manager, &version)?;
        ui::success!("Switched to version {}", ui::version(&version));
        ui::detail!("Run 'ampd --version' and 'ampctl --version' to verify installation");
        return Ok(());
    }

    ui::info!("Installing version {}", ui::version(&version));

    // Detect or override platform and architecture
    let platform = match platform_override {
        Some(p) => match p.as_str() {
            "linux" => Platform::Linux,
            "darwin" => Platform::Darwin,
            _ => {
                return Err(
                    crate::platform::PlatformError::UnsupportedPlatform { detected: p }.into(),
                );
            }
        },
        None => Platform::detect()?,
    };

    let arch = match arch_override {
        Some(a) => match a.as_str() {
            "x86_64" | "amd64" => Architecture::X86_64,
            "aarch64" | "arm64" => Architecture::Aarch64,
            _ => {
                return Err(crate::platform::PlatformError::UnsupportedArchitecture {
                    detected: a,
                }
                .into());
            }
        },
        None => Architecture::detect()?,
    };

    ui::detail!("Platform: {}, Architecture: {}", platform, arch);

    // Install the binary
    let installer = Installer::new(version_manager, github);
    installer
        .install_from_release(&version, platform, arch)
        .await?;

    ui::success!("Installed ampd and ampctl {}", ui::version(&version));
    ui::detail!("Run 'ampd --version' and 'ampctl --version' to verify installation");

    Ok(())
}
