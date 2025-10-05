use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result};
use fs_err as fs;

use crate::config::Config;

/// Represents the source from which to build nozzle
enum BuildSource {
    /// Build from a local repository path
    Local { path: PathBuf },
    /// Build from a specific branch (repo = None means default repo)
    Branch {
        repo: Option<String>,
        branch: String,
    },
    /// Build from a specific commit (repo = None means default repo)
    Commit {
        repo: Option<String>,
        commit: String,
    },
    /// Build from a pull request (repo = None means default repo)
    Pr { repo: Option<String>, number: u32 },
    /// Build from main branch (repo = None means default repo)
    Main { repo: Option<String> },
}

/// Main entry point for build command - handles all build source combinations
pub async fn run(
    path: Option<PathBuf>,
    repo: Option<String>,
    branch: Option<String>,
    commit: Option<String>,
    pr: Option<u32>,
    name: Option<String>,
    jobs: Option<usize>,
) -> Result<()> {
    // Get config to access default repo
    let config = Config::new()?;

    // Determine build source based on provided options
    let source = match (path, branch, commit, pr) {
        (Some(path), None, None, None) => BuildSource::Local { path },
        (None, Some(branch), None, None) => BuildSource::Branch { repo, branch },
        (None, None, Some(commit), None) => BuildSource::Commit { repo, commit },
        (None, None, None, Some(number)) => BuildSource::Pr { repo, number },
        (None, None, None, None) => BuildSource::Main { repo },
        _ => unreachable!("Clap should prevent conflicting options"),
    };

    println!(
        "nozzleup: Building from source: {}",
        describe_source(&source)
    );

    // Execute the build based on source type
    match source {
        BuildSource::Local { ref path } => {
            build_from_local_path(path, &source, name.as_deref(), jobs)?;
        }
        BuildSource::Branch {
            ref repo,
            ref branch,
        } => {
            let repo = repo.as_ref().unwrap_or(&config.repo);
            build_from_remote(
                repo,
                Some(branch.as_str()),
                None,
                None,
                &source,
                name.as_deref(),
                jobs,
            )
            .await?;
        }
        BuildSource::Commit {
            ref repo,
            ref commit,
        } => {
            let repo = repo.as_ref().unwrap_or(&config.repo);
            build_from_remote(
                repo,
                None,
                Some(commit.as_str()),
                None,
                &source,
                name.as_deref(),
                jobs,
            )
            .await?;
        }
        BuildSource::Pr { ref repo, number } => {
            let repo = repo.as_ref().unwrap_or(&config.repo);
            build_from_remote(
                repo,
                None,
                None,
                Some(number),
                &source,
                name.as_deref(),
                jobs,
            )
            .await?;
        }
        BuildSource::Main { ref repo } => {
            let repo = repo.as_ref().unwrap_or(&config.repo);
            build_from_remote(repo, None, None, None, &source, name.as_deref(), jobs).await?;
        }
    }

    Ok(())
}

/// Generate a human-readable description of the build source
fn describe_source(source: &BuildSource) -> String {
    match source {
        BuildSource::Local { path } => format!("local path: {}", path.display()),
        BuildSource::Branch { repo, branch } => {
            if let Some(repo) = repo {
                format!("repository: {}, branch: {}", repo, branch)
            } else {
                format!("branch: {}", branch)
            }
        }
        BuildSource::Commit { repo, commit } => {
            if let Some(repo) = repo {
                format!("repository: {}, commit: {}", repo, commit)
            } else {
                format!("commit: {}", commit)
            }
        }
        BuildSource::Pr { repo, number } => {
            if let Some(repo) = repo {
                format!("repository: {}, pull request #{}", repo, number)
            } else {
                format!("pull request #{}", number)
            }
        }
        BuildSource::Main { repo } => {
            if let Some(repo) = repo {
                format!("repository: {} (main branch)", repo)
            } else {
                "default repository (main branch)".to_string()
            }
        }
    }
}

/// Get the git commit hash from a repository path
/// Returns None if the path is not a git repository
fn get_git_commit_hash(repo_path: &Path) -> Result<Option<String>> {
    // Check if .git directory exists
    if !repo_path.join(".git").exists() {
        return Ok(None);
    }

    // Try to get the commit hash
    let output = Command::new("git")
        .args(["rev-parse", "--short=8", "HEAD"])
        .current_dir(repo_path)
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

/// Generate version label from build source, git hash, and custom name
fn generate_version_label(
    source: &BuildSource,
    git_hash: Option<&str>,
    custom_name: Option<&str>,
) -> String {
    // Custom name always takes precedence
    if let Some(name) = custom_name {
        return name.to_string();
    }

    // Generate base label
    let base = match source {
        BuildSource::Local { .. } => "local".to_string(),
        BuildSource::Branch { repo, branch } => {
            if let Some(repo) = repo {
                let slug = repo.replace('/', "-");
                format!("{}-branch-{}", slug, branch)
            } else {
                format!("branch-{}", branch)
            }
        }
        BuildSource::Commit { repo, commit } => {
            // Commit already has hash in it, don't append git hash later
            let commit_hash = &commit[..8.min(commit.len())];
            if let Some(repo) = repo {
                let slug = repo.replace('/', "-");
                return format!("{}-commit-{}", slug, commit_hash);
            } else {
                return format!("commit-{}", commit_hash);
            }
        }
        BuildSource::Pr { repo, number } => {
            if let Some(repo) = repo {
                let slug = repo.replace('/', "-");
                format!("{}-pr-{}", slug, number)
            } else {
                format!("pr-{}", number)
            }
        }
        BuildSource::Main { repo } => {
            if let Some(repo) = repo {
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

/// Build from local repository path
fn build_from_local_path(
    path: &Path,
    source: &BuildSource,
    custom_name: Option<&str>,
    jobs: Option<usize>,
) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("Local path does not exist: {}", path.display());
    }

    if !path.is_dir() {
        anyhow::bail!("Local path is not a directory: {}", path.display());
    }

    // Check for git repository and extract commit hash
    let git_hash = get_git_commit_hash(path)?;

    // If not a git repo and no custom name provided, error out
    if git_hash.is_none() && custom_name.is_none() {
        anyhow::bail!(
            "Local path is not a git repository. Use --name to specify a version name.\n\
             Example: nozzleup build --path {} --name my-version",
            path.display()
        );
    }

    // Generate version label
    let version_label = generate_version_label(source, git_hash.as_deref(), custom_name);

    let config = Config::new()?;
    build_and_install(&config, path, &version_label, jobs)?;

    Ok(())
}

/// Build from a remote repository
async fn build_from_remote(
    repo: &str,
    branch: Option<&str>,
    commit: Option<&str>,
    pr: Option<u32>,
    source: &BuildSource,
    custom_name: Option<&str>,
    jobs: Option<usize>,
) -> Result<()> {
    let config = Config::new()?;
    let temp_dir = tempfile::tempdir().context("Failed to create temporary directory")?;

    // Clone the repository
    clone_repository(repo, temp_dir.path(), branch).await?;

    // Handle commit or PR if specified
    if let Some(commit) = commit {
        checkout_commit(temp_dir.path(), commit)?;
    } else if let Some(pr) = pr {
        fetch_and_checkout_pr(temp_dir.path(), pr)?;
    }

    // Extract git commit hash
    let git_hash = get_git_commit_hash(temp_dir.path())?;

    // Generate version label
    let version_label = generate_version_label(source, git_hash.as_deref(), custom_name);

    // Build and install
    build_and_install(&config, temp_dir.path(), &version_label, jobs)?;

    Ok(())
}

/// Checkout a specific commit
fn checkout_commit(repo_path: &Path, commit: &str) -> Result<()> {
    let status = Command::new("git")
        .args(["checkout", commit])
        .current_dir(repo_path)
        .status()
        .context("Failed to execute git checkout")?;

    if !status.success() {
        anyhow::bail!("Failed to checkout commit {}", commit);
    }

    Ok(())
}

/// Fetch and checkout a pull request
fn fetch_and_checkout_pr(repo_path: &Path, number: u32) -> Result<()> {
    // Fetch the PR
    let pr_ref = format!("pull/{}/head:pr-{}", number, number);
    let status = Command::new("git")
        .args(["fetch", "origin", &pr_ref])
        .current_dir(repo_path)
        .status()
        .context("Failed to execute git fetch")?;

    if !status.success() {
        anyhow::bail!("Failed to fetch pull request #{}", number);
    }

    // Checkout the PR
    let status = Command::new("git")
        .args(["checkout", &format!("pr-{}", number)])
        .current_dir(repo_path)
        .status()
        .context("Failed to execute git checkout")?;

    if !status.success() {
        anyhow::bail!("Failed to checkout pull request #{}", number);
    }

    Ok(())
}

/// Clone a repository from GitHub
async fn clone_repository(repo: &str, destination: &Path, branch: Option<&str>) -> Result<()> {
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

    Ok(())
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
