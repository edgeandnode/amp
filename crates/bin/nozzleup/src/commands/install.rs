use anyhow::Result;

use crate::{
    config::Config,
    github::GitHubClient,
    install::Installer,
    platform::{Architecture, Platform},
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

    // Determine version to install
    let version = match version {
        Some(v) => v,
        None => {
            println!("nozzleup: Fetching latest version...");
            github.get_latest_version().await?
        }
    };

    // Check if this version is already installed
    let version_binary = config.version_binary_path(&version);
    if version_binary.exists() {
        println!("nozzleup: Version {} is already installed", version);

        // Check if it's the current version
        let current_version = config.current_version()?;
        if current_version.as_deref() == Some(&version) {
            println!("nozzleup: Already using version {}", version);
            return Ok(());
        }

        // Switch to this version
        println!("nozzleup: Switching to version {}", version);
        crate::commands::use_version::switch_to_version(&config, &version)?;
        println!("nozzleup: Successfully switched to nozzle ({})", version);
        println!("nozzleup: Run 'nozzle --version' to verify installation");
        return Ok(());
    }

    println!("nozzleup: Installing version {}", version);

    // Detect or override platform and architecture
    let platform = match platform_override {
        Some(p) => match p.as_str() {
            "linux" => Platform::Linux,
            "darwin" => Platform::Darwin,
            _ => anyhow::bail!("Unsupported platform: {}", p),
        },
        None => Platform::detect()?,
    };

    let arch = match arch_override {
        Some(a) => match a.as_str() {
            "x86_64" | "amd64" => Architecture::X86_64,
            "aarch64" | "arm64" => Architecture::Aarch64,
            _ => anyhow::bail!("Unsupported architecture: {}", a),
        },
        None => Architecture::detect()?,
    };

    println!("nozzleup: Platform: {}, Architecture: {}", platform, arch);

    // Install the binary
    let installer = Installer::new(config, github);
    installer
        .install_from_release(&version, platform, arch)
        .await?;

    println!("nozzleup: Successfully installed nozzle ({})", version);
    println!("nozzleup: Run 'nozzle --version' to verify installation");

    Ok(())
}
