use anyhow::Result;
use clap::{Parser, Subcommand};

mod commands;
mod config;
mod github;
mod install;
mod platform;

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

        /// Skip SHA256 verification (not recommended)
        #[arg(short, long)]
        force: bool,
    },

    /// List installed versions
    List,

    /// Switch to a specific installed version
    Use {
        /// Version to switch to
        version: String,
    },

    /// Uninstall a specific version
    Uninstall {
        /// Version to uninstall
        version: String,
    },

    /// Build and install from a specific branch
    Branch {
        /// Branch name to build from
        branch: String,

        /// Number of CPU cores to use when building
        #[arg(short, long)]
        jobs: Option<usize>,
    },

    /// Build and install from a specific commit
    Commit {
        /// Commit hash to build from
        commit: String,

        /// Number of CPU cores to use when building
        #[arg(short, long)]
        jobs: Option<usize>,
    },

    /// Build and install from a pull request
    Pr {
        /// Pull request number
        number: u32,

        /// Number of CPU cores to use when building
        #[arg(short, long)]
        jobs: Option<usize>,
    },

    /// Build and install from a local repository
    Path {
        /// Path to local repository
        path: std::path::PathBuf,

        /// Number of CPU cores to use when building
        #[arg(short, long)]
        jobs: Option<usize>,
    },

    /// Build and install from a remote repository
    Repo {
        /// Repository in format "owner/repo"
        repo: String,

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
        Some(Commands::Install {
            version,
            arch,
            platform,
            force,
        }) => {
            commands::install::run(version, arch, platform, force).await?;
        }
        Some(Commands::List) => {
            commands::list::run()?;
        }
        Some(Commands::Use { version }) => {
            commands::use_version::run(&version)?;
        }
        Some(Commands::Uninstall { version }) => {
            commands::uninstall::run(&version)?;
        }
        Some(Commands::Branch { branch, jobs }) => {
            commands::build::run_branch(&branch, jobs).await?;
        }
        Some(Commands::Commit { commit, jobs }) => {
            commands::build::run_commit(&commit, jobs).await?;
        }
        Some(Commands::Pr { number, jobs }) => {
            commands::build::run_pr(number, jobs).await?;
        }
        Some(Commands::Path { path, jobs }) => {
            commands::build::run_path(&path, jobs)?;
        }
        Some(Commands::Repo { repo, jobs }) => {
            commands::build::run_repo(&repo, jobs).await?;
        }
        Some(Commands::Update) => {
            commands::update::run().await?;
        }
        None => {
            // Default: install latest version
            commands::install::run(None, None, None, false).await?;
        }
    }

    Ok(())
}
