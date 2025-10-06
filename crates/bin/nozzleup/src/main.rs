use anyhow::Result;
use clap::{Parser, Subcommand};

mod commands;
mod config;
mod github;
mod install;
mod platform;
mod shell;

/// The nozzle installer and version manager
#[derive(Parser, Debug)]
#[command(name = "nozzleup")]
#[command(about = "The nozzle installer and version manager", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initialize nozzleup (called by install script)
    #[command(hide = true)]
    Init {
        /// Installation directory (default: $XDG_CONFIG_HOME/.nozzle or $HOME/.nozzle)
        #[arg(long)]
        install_dir: Option<std::path::PathBuf>,

        /// Don't modify PATH environment variable
        #[arg(long)]
        no_modify_path: bool,

        /// Don't install latest nozzle version after setup
        #[arg(long)]
        no_install_latest: bool,
    },

    /// Install a specific version from binaries (default: latest)
    Install {
        /// Version to install (e.g., v0.1.0). If not specified, installs latest
        version: Option<String>,

        /// Override architecture detection (x86_64, aarch64)
        #[arg(long)]
        arch: Option<String>,

        /// Override platform detection (linux, darwin)
        #[arg(long)]
        platform: Option<String>,
    },

    /// List installed versions
    List,

    /// Switch to a specific installed version
    Use {
        /// Version to switch to (if not provided, shows interactive selection)
        version: Option<String>,
    },

    /// Uninstall a specific version
    Uninstall {
        /// Version to uninstall
        version: String,
    },

    /// Build and install from source
    Build {
        /// Build from local repository path
        #[arg(short, long, conflicts_with_all = ["repo", "branch", "commit", "pr"])]
        path: Option<std::path::PathBuf>,

        /// GitHub repository in format "owner/repo" (default: configured repo)
        #[arg(short, long, conflicts_with = "path")]
        repo: Option<String>,

        /// Build from specific branch
        #[arg(short, long, conflicts_with_all = ["path", "commit", "pr"])]
        branch: Option<String>,

        /// Build from specific commit hash
        #[arg(short = 'C', long, conflicts_with_all = ["path", "branch", "pr"])]
        commit: Option<String>,

        /// Build from pull request number
        #[arg(short = 'P', long, conflicts_with_all = ["path", "branch", "commit"])]
        pr: Option<u32>,

        /// Custom version name (required for non-git local paths, optional otherwise)
        #[arg(short, long)]
        name: Option<String>,

        /// Number of CPU cores to use when building
        #[arg(short, long)]
        jobs: Option<usize>,
    },

    /// Update nozzleup itself
    Update,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Init {
            install_dir,
            no_modify_path,
            no_install_latest,
        }) => {
            commands::init::run(install_dir, no_modify_path, no_install_latest).await?;
        }
        Some(Commands::Install {
            version,
            arch,
            platform,
        }) => {
            commands::install::run(version, arch, platform).await?;
        }
        Some(Commands::List) => {
            commands::list::run()?;
        }
        Some(Commands::Use { version }) => {
            commands::use_version::run(version)?;
        }
        Some(Commands::Uninstall { version }) => {
            commands::uninstall::run(&version)?;
        }
        Some(Commands::Build {
            path,
            repo,
            branch,
            commit,
            pr,
            name,
            jobs,
        }) => {
            commands::build::run(path, repo, branch, commit, pr, name, jobs).await?;
        }
        Some(Commands::Update) => {
            commands::update::run().await?;
        }
        None => {
            // Default: install latest version
            commands::install::run(None, None, None).await?;
        }
    }

    Ok(())
}
