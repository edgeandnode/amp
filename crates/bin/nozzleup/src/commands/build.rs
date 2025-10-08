use std::{
    fmt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result};
use fs_err as fs;

use crate::{DEFAULT_REPO, config::Config};

/// Represents the source from which to build nozzle
enum BuildSource {
    /// Build from a local repository path
    Local { path: PathBuf },
    /// Build from a specific branch
    Branch { repo: String, branch: String },
    /// Build from a specific commit
    Commit { repo: String, commit: String },
    /// Build from a pull request
    Pr { repo: String, number: u32 },
    /// Build from main branch
    Main { repo: String },
}

impl BuildSource {
    /// Generate version label for this build source
    fn generate_version_label(&self, git_hash: Option<&str>, custom_name: Option<&str>) -> String {
        // Custom name always takes precedence
        if let Some(name) = custom_name {
            return name.to_string();
        }

        // Generate base label
        let base = match self {
            Self::Local { .. } => "local".to_string(),
            Self::Branch { repo, branch } => {
                if repo != DEFAULT_REPO {
                    let slug = repo.replace('/', "-");
                    format!("{}-branch-{}", slug, branch)
                } else {
                    format!("branch-{}", branch)
                }
            }
            Self::Commit { repo, commit } => {
                // Commit already has hash in it, don't append git hash later
                let commit_hash = &commit[..8.min(commit.len())];
                if repo != DEFAULT_REPO {
                    let slug = repo.replace('/', "-");
                    return format!("{}-commit-{}", slug, commit_hash);
                } else {
                    return format!("commit-{}", commit_hash);
                }
            }
            Self::Pr { repo, number } => {
                if repo != DEFAULT_REPO {
                    let slug = repo.replace('/', "-");
                    format!("{}-pr-{}", slug, number)
                } else {
                    format!("pr-{}", number)
                }
            }
            Self::Main { repo } => {
                if repo != DEFAULT_REPO {
                    let slug = repo.replace('/', "-");
                    format!("{}-main", slug)
                } else {
                    "main".to_string()
                }
            }
        };

        // Append git hash if available
        if let Some(hash) = git_hash {
            format!("{}-{}", base, hash)
        } else {
            base
        }
    }

    /// Execute the build for this source
    pub async fn build(
        &self,
        install_dir: Option<PathBuf>,
        custom_name: Option<&str>,
        jobs: Option<usize>,
    ) -> Result<()> {
        let config = Config::new(install_dir)?;

        match self {
            Self::Local { path } => {
                // Validate path exists and is a directory
                if !path.exists() {
                    anyhow::bail!("Local path does not exist: {}", path.display());
                }
                if !path.is_dir() {
                    anyhow::bail!("Local path is not a directory: {}", path.display());
                }

                // Check for git repository and extract commit hash
                let git = GitRepo::new(path);
                let git_hash = git.get_commit_hash()?;

                // If not a git repo and no custom name provided, error out
                if git_hash.is_none() && custom_name.is_none() {
                    anyhow::bail!(
                        "Local path is not a git repository. Use --name to specify a version name.\n\
                         Example: nozzleup build --path {} --name my-version",
                        path.display()
                    );
                }

                // Generate version label and build
                let version_label = self.generate_version_label(git_hash.as_deref(), custom_name);
                build_and_install(&config, path, &version_label, jobs)?;
            }
            Self::Branch { repo, branch } => {
                let temp_dir =
                    tempfile::tempdir().context("Failed to create temporary directory")?;

                // Clone repository with specific branch
                let git = GitRepo::clone(repo, temp_dir.path(), Some(branch.as_str())).await?;

                // Extract git commit hash, generate version label, and build
                let git_hash = git.get_commit_hash()?;
                let version_label = self.generate_version_label(git_hash.as_deref(), custom_name);
                build_and_install(&config, temp_dir.path(), &version_label, jobs)?;
            }
            Self::Commit { repo, commit } => {
                let temp_dir =
                    tempfile::tempdir().context("Failed to create temporary directory")?;

                // Clone repository and checkout specific commit
                let git = GitRepo::clone(repo, temp_dir.path(), None).await?;
                git.checkout_commit(commit)?;

                // Extract git commit hash, generate version label, and build
                let git_hash = git.get_commit_hash()?;
                let version_label = self.generate_version_label(git_hash.as_deref(), custom_name);
                build_and_install(&config, temp_dir.path(), &version_label, jobs)?;
            }
            Self::Pr { repo, number } => {
                let temp_dir =
                    tempfile::tempdir().context("Failed to create temporary directory")?;

                // Clone repository and checkout pull request
                let git = GitRepo::clone(repo, temp_dir.path(), None).await?;
                git.fetch_and_checkout_pr(*number)?;

                // Extract git commit hash, generate version label, and build
                let git_hash = git.get_commit_hash()?;
                let version_label = self.generate_version_label(git_hash.as_deref(), custom_name);
                build_and_install(&config, temp_dir.path(), &version_label, jobs)?;
            }
            Self::Main { repo } => {
                let temp_dir =
                    tempfile::tempdir().context("Failed to create temporary directory")?;

                // Clone repository (main branch)
                let git = GitRepo::clone(repo, temp_dir.path(), None).await?;

                // Extract git commit hash, generate version label, and build
                let git_hash = git.get_commit_hash()?;
                let version_label = self.generate_version_label(git_hash.as_deref(), custom_name);
                build_and_install(&config, temp_dir.path(), &version_label, jobs)?;
            }
        }

        Ok(())
    }
}

impl fmt::Display for BuildSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local { path } => write!(f, "local path: {}", path.display()),
            Self::Branch { repo, branch } => {
                if repo != DEFAULT_REPO {
                    write!(f, "repository: {}, branch: {}", repo, branch)
                } else {
                    write!(f, "branch: {}", branch)
                }
            }
            Self::Commit { repo, commit } => {
                if repo != DEFAULT_REPO {
                    write!(f, "repository: {}, commit: {}", repo, commit)
                } else {
                    write!(f, "commit: {}", commit)
                }
            }
            Self::Pr { repo, number } => {
                if repo != DEFAULT_REPO {
                    write!(f, "repository: {}, pull request #{}", repo, number)
                } else {
                    write!(f, "pull request #{}", number)
                }
            }
            Self::Main { repo } => {
                if repo != DEFAULT_REPO {
                    write!(f, "repository: {} (main branch)", repo)
                } else {
                    write!(f, "default repository (main branch)")
                }
            }
        }
    }
}

/// Main entry point for build command - handles all build source combinations
pub async fn run(
    install_dir: Option<PathBuf>,
    repo: Option<String>,
    path: Option<PathBuf>,
    branch: Option<String>,
    commit: Option<String>,
    pr: Option<u32>,
    name: Option<String>,
    jobs: Option<usize>,
) -> Result<()> {
    // Determine build source based on provided options
    let source = match (path, repo, branch, commit, pr) {
        (Some(path), _, None, None, None) => BuildSource::Local { path },
        (None, Some(repo), Some(branch), None, None) => BuildSource::Branch { repo, branch },
        (None, Some(repo), None, Some(commit), None) => BuildSource::Commit { repo, commit },
        (None, Some(repo), None, None, Some(number)) => BuildSource::Pr { repo, number },
        (None, Some(repo), None, None, None) => BuildSource::Main { repo },
        (None, None, Some(branch), None, None) => BuildSource::Branch {
            repo: DEFAULT_REPO.to_string(),
            branch,
        },
        (None, None, None, Some(commit), None) => BuildSource::Commit {
            repo: DEFAULT_REPO.to_string(),
            commit,
        },
        (None, None, None, None, Some(number)) => BuildSource::Pr {
            repo: DEFAULT_REPO.to_string(),
            number,
        },
        (None, None, None, None, None) => BuildSource::Main {
            repo: DEFAULT_REPO.to_string(),
        },
        _ => unreachable!("Clap should prevent conflicting options"),
    };

    println!("nozzleup: Building from source: {}", source);

    // Execute the build
    source.build(install_dir, name.as_deref(), jobs).await?;

    Ok(())
}

/// Git repository operations
struct GitRepo<'a> {
    path: &'a Path,
    remote: String,
}

impl<'a> GitRepo<'a> {
    /// Create a new GitRepo instance for an existing repository
    fn new(path: &'a Path) -> Self {
        Self {
            path,
            remote: "origin".to_string(),
        }
    }

    /// Clone a repository from GitHub and create a GitRepo instance
    async fn clone(repo: &str, destination: &'a Path, branch: Option<&str>) -> Result<Self> {
        check_command_exists("git")?;

        let repo_url = format!("https://github.com/{}.git", repo);

        println!("nozzleup: Cloning {}...", repo_url);

        let mut args = vec!["clone"];

        if let Some(branch) = branch {
            args.extend(["--branch", branch]);
        }

        args.push(&repo_url);
        args.push(destination.to_str().unwrap());

        let status = Command::new("git")
            .args(&args)
            .status()
            .context("Failed to execute git clone")?;

        if !status.success() {
            anyhow::bail!("Failed to clone repository {}", repo);
        }

        Ok(Self::new(destination))
    }

    /// Get the commit hash from this repository
    /// Returns None if the path is not a git repository
    fn get_commit_hash(&self) -> Result<Option<String>> {
        // Check if .git directory exists
        if !self.path.join(".git").exists() {
            return Ok(None);
        }

        // Try to get the commit hash
        let output = Command::new("git")
            .args(["rev-parse", "--short=8", "HEAD"])
            .current_dir(self.path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("Failed to execute git rev-parse")?;

        if !output.status.success() {
            return Ok(None);
        }

        let hash = String::from_utf8(output.stdout)
            .context("Failed to parse git output")?
            .trim()
            .to_string();

        Ok(Some(hash))
    }

    /// Checkout a specific commit
    fn checkout_commit(&self, commit: &str) -> Result<()> {
        let status = Command::new("git")
            .args(["checkout", commit])
            .current_dir(self.path)
            .status()
            .context("Failed to execute git checkout")?;

        if !status.success() {
            anyhow::bail!("Failed to checkout commit {}", commit);
        }

        Ok(())
    }

    /// Fetch and checkout a pull request
    fn fetch_and_checkout_pr(&self, number: u32) -> Result<()> {
        // Fetch the PR
        let pr_ref = format!("pull/{}/head:pr-{}", number, number);
        let status = Command::new("git")
            .args(["fetch", &self.remote, &pr_ref])
            .current_dir(self.path)
            .status()
            .context("Failed to execute git fetch")?;

        if !status.success() {
            anyhow::bail!("Failed to fetch pull request #{}", number);
        }

        // Checkout the PR
        let status = Command::new("git")
            .args(["checkout", &format!("pr-{}", number)])
            .current_dir(self.path)
            .status()
            .context("Failed to execute git checkout")?;

        if !status.success() {
            anyhow::bail!("Failed to checkout pull request #{}", number);
        }

        Ok(())
    }
}

/// Build and install the nozzle binary
fn build_and_install(
    config: &Config,
    repo_path: &Path,
    version_label: &str,
    jobs: Option<usize>,
) -> Result<()> {
    check_command_exists("cargo")?;

    println!("nozzleup: Building nozzle...");

    let mut args = vec!["build", "--release", "-p", "nozzle"];

    let jobs_str;
    if let Some(j) = jobs {
        jobs_str = j.to_string();
        args.extend(["-j", &jobs_str]);
    }

    let status = Command::new("cargo")
        .args(&args)
        .current_dir(repo_path)
        .status()
        .context("Failed to execute cargo build")?;

    if !status.success() {
        anyhow::bail!("Build failed");
    }

    // Find the built binary
    let binary_source = repo_path.join("target/release/nozzle");

    if !binary_source.exists() {
        anyhow::bail!(
            "Build succeeded but binary not found at {}",
            binary_source.display()
        );
    }

    // Create version directory
    let version_dir = config.versions_dir.join(version_label);
    fs::create_dir_all(&version_dir).context("Failed to create version directory")?;

    let binary_dest = version_dir.join("nozzle");

    // Copy the binary
    fs::copy(&binary_source, &binary_dest).context("Failed to copy binary")?;

    // Make it executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&binary_dest)
            .context("Failed to get binary metadata")?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&binary_dest, perms).context("Failed to set executable permissions")?;
    }

    // Create symlink to active binary
    let active_path = config.active_binary_path();

    // Remove existing symlink if it exists
    if active_path.exists() || active_path.is_symlink() {
        fs::remove_file(&active_path).context("Failed to remove existing symlink")?;
    }

    // Create new symlink
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        symlink(&binary_dest, &active_path).context("Failed to create symlink")?;
    }

    // Update current version
    config.set_current_version(version_label)?;

    println!(
        "nozzleup: Successfully built and installed nozzle ({})",
        version_label
    );
    println!("nozzleup: Run 'nozzle --version' to verify installation");

    Ok(())
}

/// Check if a command exists
fn check_command_exists(command: &str) -> Result<()> {
    let status = Command::new(command)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    match status {
        Ok(_) => Ok(()),
        Err(_) => anyhow::bail!("Command '{}' not found. Please install it first.", command),
    }
}
