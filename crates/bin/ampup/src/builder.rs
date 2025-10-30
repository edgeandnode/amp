use std::{
    fmt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result};
use fs_err as fs;

use crate::{DEFAULT_REPO, ui, version_manager::VersionManager};

#[derive(Debug)]
pub enum BuildError {
    LocalPathNotFound {
        path: PathBuf,
    },
    LocalPathNotDirectory {
        path: PathBuf,
    },
    LocalPathNotGitRepo {
        path: PathBuf,
    },
    GitCloneFailed {
        repo: String,
        branch: Option<String>,
    },
    GitCheckoutFailed {
        target: String,
    },
    GitFetchPrFailed {
        pr: u32,
    },
    CargoBuildFailed,
    BinaryNotFound {
        path: PathBuf,
    },
    CommandNotFound {
        command: String,
    },
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalPathNotFound { path } => {
                writeln!(f, "Local path does not exist")?;
                writeln!(f, "  Path: {}", path.display())?;
                writeln!(f)?;
                writeln!(f, "  Check that the path is correct and accessible.")?;
            }
            Self::LocalPathNotDirectory { path } => {
                writeln!(f, "Local path is not a directory")?;
                writeln!(f, "  Path: {}", path.display())?;
                writeln!(f)?;
                writeln!(
                    f,
                    "  The build command requires a directory containing a Cargo workspace."
                )?;
            }
            Self::LocalPathNotGitRepo { path } => {
                writeln!(f, "Local path is not a git repository")?;
                writeln!(f, "  Path: {}", path.display())?;
                writeln!(f)?;
                writeln!(
                    f,
                    "  Use --name flag to specify a version name for non-git builds."
                )?;
                writeln!(
                    f,
                    "  Example: ampup build --path {} --name my-version",
                    path.display()
                )?;
            }
            Self::GitCloneFailed { repo, branch } => {
                writeln!(f, "Failed to clone repository")?;
                writeln!(f, "  Repository: {}", repo)?;
                if let Some(b) = branch {
                    writeln!(f, "  Branch: {}", b)?;
                }
                writeln!(f)?;
                writeln!(f, "  Ensure the repository exists and is accessible.")?;
                writeln!(f, "  Check your network connection and GitHub permissions.")?;
            }
            Self::GitCheckoutFailed { target } => {
                writeln!(f, "Failed to checkout git reference")?;
                writeln!(f, "  Target: {}", target)?;
                writeln!(f)?;
                writeln!(f, "  The commit/branch may not exist in the repository.")?;
            }
            Self::GitFetchPrFailed { pr } => {
                writeln!(f, "Failed to fetch pull request")?;
                writeln!(f, "  PR: #{}", pr)?;
                writeln!(f)?;
                writeln!(f, "  Ensure the pull request exists and is accessible.")?;
            }
            Self::CargoBuildFailed => {
                writeln!(f, "Cargo build failed")?;
                writeln!(f)?;
                writeln!(f, "  Check the build output above for compilation errors.")?;
                writeln!(
                    f,
                    "  Ensure all dependencies are installed and the code compiles."
                )?;
            }
            Self::BinaryNotFound { path } => {
                writeln!(f, "Binary not found after build")?;
                writeln!(f, "  Expected: {}", path.display())?;
                writeln!(f)?;
                writeln!(f, "  Build succeeded but the ampd binary was not created.")?;
                writeln!(
                    f,
                    "  This may indicate an issue with the build configuration."
                )?;
            }
            Self::CommandNotFound { command } => {
                writeln!(f, "Required command not found")?;
                writeln!(f, "  Command: {}", command)?;
                writeln!(f)?;
                match command.as_str() {
                    "git" => {
                        writeln!(f, "  Install git:")?;
                        writeln!(f, "    macOS: brew install git")?;
                        writeln!(f, "    Ubuntu/Debian: sudo apt install git")?;
                    }
                    "cargo" => {
                        writeln!(f, "  Install Rust toolchain:")?;
                        writeln!(
                            f,
                            "    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
                        )?;
                    }
                    _ => {
                        writeln!(
                            f,
                            "  Please install {} and ensure it's in your PATH.",
                            command
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl std::error::Error for BuildError {}

/// Represents the source from which to build ampd
pub enum BuildSource {
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
    fn generate_version_label(&self, git_hash: Option<&str>, name: Option<&str>) -> String {
        // Custom name always takes precedence
        if let Some(name) = name {
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

/// Options for building ampd
pub struct BuildOptions {
    /// Custom version name
    pub name: Option<String>,
    /// Number of CPU cores to use
    pub jobs: Option<usize>,
}

/// Builder for ampd from source
pub struct Builder {
    version_manager: VersionManager,
}

impl Builder {
    pub fn new(version_manager: VersionManager) -> Self {
        Self { version_manager }
    }

    /// Execute the build for a given source
    pub async fn build(&self, source: BuildSource, options: BuildOptions) -> Result<()> {
        match &source {
            BuildSource::Local { path } => {
                // Validate path exists and is a directory
                if !path.exists() {
                    return Err(BuildError::LocalPathNotFound { path: path.clone() }.into());
                }
                if !path.is_dir() {
                    return Err(BuildError::LocalPathNotDirectory { path: path.clone() }.into());
                }

                // Check for git repository and extract commit hash
                let git = GitRepo::new(path);
                let git_hash = git.get_commit_hash()?;

                // If not a git repo and no custom name provided, error out
                if git_hash.is_none() && options.name.is_none() {
                    return Err(BuildError::LocalPathNotGitRepo { path: path.clone() }.into());
                }

                // Generate version label and build
                let version_label =
                    source.generate_version_label(git_hash.as_deref(), options.name.as_deref());
                build_and_install(&self.version_manager, path, &version_label, options.jobs)?;

                Ok(())
            }
            BuildSource::Branch { repo, branch } => {
                let temp_dir =
                    tempfile::tempdir().context("Failed to create temporary directory")?;

                // Clone repository with specific branch
                let git = GitRepo::clone(repo, temp_dir.path(), Some(branch.as_str())).await?;

                // Extract git commit hash, generate version label, and build
                let git_hash = git.get_commit_hash()?;
                let version_label =
                    source.generate_version_label(git_hash.as_deref(), options.name.as_deref());
                build_and_install(
                    &self.version_manager,
                    temp_dir.path(),
                    &version_label,
                    options.jobs,
                )?;

                Ok(())
            }
            BuildSource::Commit { repo, commit } => {
                let temp_dir =
                    tempfile::tempdir().context("Failed to create temporary directory")?;

                // Clone repository and checkout specific commit
                let git = GitRepo::clone(repo, temp_dir.path(), None).await?;
                git.checkout_commit(commit)?;

                // Extract git commit hash, generate version label, and build
                let git_hash = git.get_commit_hash()?;
                let version_label =
                    source.generate_version_label(git_hash.as_deref(), options.name.as_deref());
                build_and_install(
                    &self.version_manager,
                    temp_dir.path(),
                    &version_label,
                    options.jobs,
                )?;

                Ok(())
            }
            BuildSource::Pr { repo, number } => {
                let temp_dir =
                    tempfile::tempdir().context("Failed to create temporary directory")?;

                // Clone repository and checkout pull request
                let git = GitRepo::clone(repo, temp_dir.path(), None).await?;
                git.fetch_and_checkout_pr(*number)?;

                // Extract git commit hash, generate version label, and build
                let git_hash = git.get_commit_hash()?;
                let version_label =
                    source.generate_version_label(git_hash.as_deref(), options.name.as_deref());
                build_and_install(
                    &self.version_manager,
                    temp_dir.path(),
                    &version_label,
                    options.jobs,
                )?;

                Ok(())
            }
            BuildSource::Main { repo } => {
                let temp_dir =
                    tempfile::tempdir().context("Failed to create temporary directory")?;

                // Clone repository (main branch)
                let git = GitRepo::clone(repo, temp_dir.path(), None).await?;

                // Extract git commit hash, generate version label, and build
                let git_hash = git.get_commit_hash()?;
                let version_label =
                    source.generate_version_label(git_hash.as_deref(), options.name.as_deref());
                build_and_install(
                    &self.version_manager,
                    temp_dir.path(),
                    &version_label,
                    options.jobs,
                )?;

                Ok(())
            }
        }
    }
}

/// Git repository operations
pub struct GitRepo<'a> {
    path: &'a Path,
    remote: String,
}

impl<'a> GitRepo<'a> {
    /// Create a new GitRepo instance for an existing repository
    pub fn new(path: &'a Path) -> Self {
        Self {
            path,
            remote: "origin".to_string(),
        }
    }

    /// Clone a repository from GitHub and create a GitRepo instance
    pub async fn clone(repo: &str, destination: &'a Path, branch: Option<&str>) -> Result<Self> {
        check_command_exists("git")?;

        let repo_url = format!("https://github.com/{}.git", repo);

        ui::info!("Cloning {}", repo_url);

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
            return Err(BuildError::GitCloneFailed {
                repo: repo.to_string(),
                branch: branch.map(|s| s.to_string()),
            }
            .into());
        }

        Ok(Self::new(destination))
    }

    /// Get the commit hash from this repository
    /// Returns None if the path is not a git repository
    pub fn get_commit_hash(&self) -> Result<Option<String>> {
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
    pub fn checkout_commit(&self, commit: &str) -> Result<()> {
        let status = Command::new("git")
            .args(["checkout", commit])
            .current_dir(self.path)
            .status()
            .context("Failed to execute git checkout")?;

        if !status.success() {
            return Err(BuildError::GitCheckoutFailed {
                target: commit.to_string(),
            }
            .into());
        }

        Ok(())
    }

    /// Fetch and checkout a pull request
    pub fn fetch_and_checkout_pr(&self, number: u32) -> Result<()> {
        // Fetch the PR
        let pr_ref = format!("pull/{}/head:pr-{}", number, number);
        let status = Command::new("git")
            .args(["fetch", &self.remote, &pr_ref])
            .current_dir(self.path)
            .status()
            .context("Failed to execute git fetch")?;

        if !status.success() {
            return Err(BuildError::GitFetchPrFailed { pr: number }.into());
        }

        // Checkout the PR
        let status = Command::new("git")
            .args(["checkout", &format!("pr-{}", number)])
            .current_dir(self.path)
            .status()
            .context("Failed to execute git checkout")?;

        if !status.success() {
            return Err(BuildError::GitCheckoutFailed {
                target: format!("PR #{}", number),
            }
            .into());
        }

        Ok(())
    }
}

/// Build and install the ampd and ampctl binaries
fn build_and_install(
    version_manager: &VersionManager,
    repo_path: &Path,
    version_label: &str,
    jobs: Option<usize>,
) -> Result<()> {
    check_command_exists("cargo")?;

    ui::info!("Building ampd and ampctl");

    let mut args = vec!["build", "--release", "-p", "ampd", "-p", "ampctl"];

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
        return Err(BuildError::CargoBuildFailed.into());
    }

    // Find the built binaries
    let ampd_source = repo_path.join("target/release/ampd");
    let ampctl_source = repo_path.join("target/release/ampctl");

    if !ampd_source.exists() {
        return Err(BuildError::BinaryNotFound {
            path: ampd_source.clone(),
        }
        .into());
    }

    if !ampctl_source.exists() {
        return Err(BuildError::BinaryNotFound {
            path: ampctl_source.clone(),
        }
        .into());
    }

    let config = version_manager.config();

    // Create version directory
    let version_dir = config.versions_dir.join(version_label);
    fs::create_dir_all(&version_dir).context("Failed to create version directory")?;

    // Copy ampd binary
    let ampd_dest = version_dir.join("ampd");
    fs::copy(&ampd_source, &ampd_dest).context("Failed to copy ampd binary")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&ampd_dest)
            .context("Failed to get ampd metadata")?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&ampd_dest, perms)
            .context("Failed to set executable permissions on ampd")?;
    }

    // Copy ampctl binary
    let ampctl_dest = version_dir.join("ampctl");
    fs::copy(&ampctl_source, &ampctl_dest).context("Failed to copy ampctl binary")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&ampctl_dest)
            .context("Failed to get ampctl metadata")?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&ampctl_dest, perms)
            .context("Failed to set executable permissions on ampctl")?;
    }

    // Activate this version
    version_manager.activate(version_label)?;

    ui::success!(
        "Built and installed ampd and ampctl {}",
        ui::version(version_label)
    );
    ui::detail!("Run 'ampd --version' and 'ampctl --version' to verify installation");

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
        Err(_) => Err(BuildError::CommandNotFound {
            command: command.to_string(),
        }
        .into()),
    }
}
