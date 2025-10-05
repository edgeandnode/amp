use anyhow::Result;

use crate::{
    config::Config,
    github::GitHubClient,
    install::Installer,
    platform::{Architecture, Platform},
};

pub async fn run(
    version: Option<String>,
    arch_override: Option<String>,
    platform_override: Option<String>,
) -> Result<()> {
    let config = Config::new()?;
    let github = GitHubClient::new(&config)?;

    // Determine version to install
    let version = match version {
        Some(v) => v,
        None => {
            println!("nozzleup: Fetching latest version...");
            github.get_latest_version().await?
        }
    };

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
