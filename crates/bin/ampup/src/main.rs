use ampup::{DEFAULT_REPO, commands};
use console::style;

/// The ampd installer and version manager
#[derive(Debug, clap::Parser)]
#[command(name = "ampup")]
#[command(about = "The ampd installer and version manager", long_about = None)]
#[command(version = env!("VERGEN_GIT_DESCRIBE"))]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// Initialize ampup (called by install script)
    #[command(hide = true)]
    Init {
        /// Installation directory (defaults to $AMP_DIR or $XDG_CONFIG_HOME/.amp or $HOME/.amp)
        #[arg(long, env = "AMP_DIR")]
        install_dir: Option<std::path::PathBuf>,

        /// Don't modify PATH environment variable
        #[arg(long)]
        no_modify_path: bool,

        /// Don't install latest ampd version after setup
        #[arg(long)]
        no_install_latest: bool,

        /// GitHub token for private repository access (defaults to $GITHUB_TOKEN)
        #[arg(long, env = "GITHUB_TOKEN", hide_env = true)]
        github_token: Option<String>,
    },

    /// Install a specific version from binaries (default: latest)
    Install {
        /// Installation directory (defaults to $AMP_DIR or $XDG_CONFIG_HOME/.amp or $HOME/.amp)
        #[arg(long, env = "AMP_DIR")]
        install_dir: Option<std::path::PathBuf>,

        /// Version to install (e.g., v0.1.0). If not specified, installs latest
        version: Option<String>,

        /// GitHub repository in format "owner/repo"
        #[arg(long, default_value_t = DEFAULT_REPO.to_string())]
        repo: String,

        /// GitHub token for private repository access (defaults to $GITHUB_TOKEN)
        #[arg(long, env = "GITHUB_TOKEN", hide_env = true)]
        github_token: Option<String>,

        /// Override architecture detection (x86_64, aarch64)
        #[arg(long)]
        arch: Option<String>,

        /// Override platform detection (linux, darwin)
        #[arg(long)]
        platform: Option<String>,

        /// Number of concurrent downloads (default: 4)
        #[arg(short = 'j', long = "jobs")]
        jobs: Option<usize>,
    },

    /// List installed versions
    List {
        /// Installation directory (defaults to $AMP_DIR or $XDG_CONFIG_HOME/.amp or $HOME/.amp)
        #[arg(long, env = "AMP_DIR")]
        install_dir: Option<std::path::PathBuf>,
    },

    /// Switch to a specific installed version
    Use {
        /// Installation directory (defaults to $AMP_DIR or $XDG_CONFIG_HOME/.amp or $HOME/.amp)
        #[arg(long, env = "AMP_DIR")]
        install_dir: Option<std::path::PathBuf>,

        /// Version to switch to (if not provided, shows interactive selection)
        version: Option<String>,
    },

    /// Uninstall a specific version
    Uninstall {
        /// Installation directory (defaults to $AMP_DIR or $XDG_CONFIG_HOME/.amp or $HOME/.amp)
        #[arg(long, env = "AMP_DIR")]
        install_dir: Option<std::path::PathBuf>,

        /// Version to uninstall
        version: String,
    },

    /// Build and install from source
    Build {
        /// Installation directory (defaults to $AMP_DIR or $XDG_CONFIG_HOME/.amp or $HOME/.amp)
        #[arg(long, env = "AMP_DIR")]
        install_dir: Option<std::path::PathBuf>,

        /// Build from local repository path
        #[arg(short, long, conflicts_with_all = ["repo", "branch", "commit", "pr"])]
        path: Option<std::path::PathBuf>,

        /// GitHub repository in format "owner/repo"
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

    /// Update to the latest ampd version (default behavior)
    Update {
        /// Installation directory (defaults to $AMP_DIR or $XDG_CONFIG_HOME/.amp or $HOME/.amp)
        #[arg(long, env = "AMP_DIR")]
        install_dir: Option<std::path::PathBuf>,

        /// GitHub repository in format "owner/repo"
        #[arg(long, default_value_t = DEFAULT_REPO.to_string())]
        repo: String,

        /// GitHub token for private repository access (defaults to $GITHUB_TOKEN)
        #[arg(long, env = "GITHUB_TOKEN", hide_env = true)]
        github_token: Option<String>,

        /// Override architecture detection (x86_64, aarch64)
        #[arg(long)]
        arch: Option<String>,

        /// Override platform detection (linux, darwin)
        #[arg(long)]
        platform: Option<String>,

        /// Number of concurrent downloads (default: 4)
        #[arg(short = 'j', long = "jobs")]
        jobs: Option<usize>,
    },

    /// Manage the ampup executable
    #[command(name = "self")]
    SelfCmd {
        #[command(subcommand)]
        command: SelfCommands,
    },
}

#[derive(Debug, clap::Subcommand)]
enum SelfCommands {
    /// Update ampup itself to the latest version
    Update {
        /// GitHub repository in format "owner/repo"
        #[arg(long, default_value_t = DEFAULT_REPO.to_string())]
        repo: String,

        /// GitHub token for private repository access (defaults to $GITHUB_TOKEN)
        #[arg(long, env = "GITHUB_TOKEN", hide_env = true)]
        github_token: Option<String>,
    },

    /// Print the version of ampup
    Version,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        // Print the error with some custom formatting
        eprintln!("{} {}", style("✗").red().bold(), e);
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = <Cli as clap::Parser>::parse();

    match cli.command {
        Some(Commands::Init {
            install_dir,
            no_modify_path,
            no_install_latest,
            github_token,
        }) => {
            commands::init::run(install_dir, no_modify_path, no_install_latest, github_token)
                .await?;
        }
        Some(Commands::Install {
            install_dir,
            version,
            repo,
            github_token,
            arch,
            platform,
            jobs,
        }) => {
            commands::install::run(
                install_dir,
                repo,
                github_token,
                version,
                arch,
                platform,
                jobs,
            )
            .await?;
        }
        Some(Commands::List { install_dir }) => {
            commands::list::run(install_dir)?;
        }
        Some(Commands::Use {
            install_dir,
            version,
        }) => {
            commands::use_version::run(install_dir, version)?;
        }
        Some(Commands::Uninstall {
            install_dir,
            version,
        }) => {
            commands::uninstall::run(install_dir, &version)?;
        }
        Some(Commands::Build {
            install_dir,
            path,
            repo,
            branch,
            commit,
            pr,
            name,
            jobs,
        }) => {
            commands::build::run(install_dir, repo, path, branch, commit, pr, name, jobs).await?;
        }
        Some(Commands::Update {
            install_dir,
            repo,
            github_token,
            arch,
            platform,
            jobs,
        }) => {
            // Install latest version (same as default behavior)
            commands::install::run(install_dir, repo, github_token, None, arch, platform, jobs)
                .await?;
        }
        Some(Commands::SelfCmd { command }) => match command {
            SelfCommands::Update { repo, github_token } => {
                commands::update::run(repo, github_token).await?;
            }
            SelfCommands::Version => {
                println!("ampup {}", env!("VERGEN_GIT_DESCRIBE"));
            }
        },
        None => {
            // Default: install latest version (same as 'ampup update')
            commands::install::run(
                std::env::var("AMP_DIR").ok().map(std::path::PathBuf::from),
                DEFAULT_REPO.to_string(),
                std::env::var("GITHUB_TOKEN").ok(),
                None,
                None,
                None,
                None,
            )
            .await?;
        }
    }

    Ok(())
}
