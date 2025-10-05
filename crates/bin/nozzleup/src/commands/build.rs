use std::{path::Path, process::Command};

use anyhow::{Context, Result};

use crate::config::Config;

/// Build and install from a branch
pub async fn run_branch(branch: &str, jobs: Option<usize>) -> Result<()> {
    println!("nozzleup: Building from branch: {}", branch);

    let config = Config::new()?;
    let temp_dir = tempfile::tempdir().context("Failed to create temporary directory")?;

    // Clone the repository
    clone_repo(&config, temp_dir.path(), Some(branch)).await?;

    // Build and install
    let version_label = format!("branch-{}", branch);
    build_and_install(&config, temp_dir.path(), &version_label, jobs)?;

    Ok(())
}

/// Build and install from a commit
pub async fn run_commit(commit: &str, jobs: Option<usize>) -> Result<()> {
    println!("nozzleup: Building from commit: {}", commit);

    let config = Config::new()?;
    let temp_dir = tempfile::tempdir().context("Failed to create temporary directory")?;

    // Clone the repository
    clone_repo(&config, temp_dir.path(), None).await?;

    // Checkout the commit
    let status = Command::new("git")
        .args(["checkout", commit])
        .current_dir(temp_dir.path())
        .status()
        .context("Failed to execute git checkout")?;

    if !status.success() {
        anyhow::bail!("Failed to checkout commit {}", commit);
    }

    // Build and install
    let version_label = format!("commit-{}", &commit[..8.min(commit.len())]);
    build_and_install(&config, temp_dir.path(), &version_label, jobs)?;

    Ok(())
}

/// Build and install from a pull request
pub async fn run_pr(number: u32, jobs: Option<usize>) -> Result<()> {
    println!("nozzleup: Building from pull request #{}", number);

    let config = Config::new()?;
    let temp_dir = tempfile::tempdir().context("Failed to create temporary directory")?;

    // Clone the repository
    clone_repo(&config, temp_dir.path(), None).await?;

    // Fetch and checkout the PR
    let pr_ref = format!("pull/{}/head:pr-{}", number, number);
    let status = Command::new("git")
        .args(["fetch", "origin", &pr_ref])
        .current_dir(temp_dir.path())
        .status()
        .context("Failed to execute git fetch")?;

    if !status.success() {
        anyhow::bail!("Failed to fetch pull request #{}", number);
    }

    let status = Command::new("git")
        .args(["checkout", &format!("pr-{}", number)])
        .current_dir(temp_dir.path())
        .status()
        .context("Failed to execute git checkout")?;

    if !status.success() {
        anyhow::bail!("Failed to checkout pull request #{}", number);
    }

    // Build and install
    let version_label = format!("pr-{}", number);
    build_and_install(&config, temp_dir.path(), &version_label, jobs)?;

    Ok(())
}

/// Build and install from a local path
pub fn run_path(path: &Path, jobs: Option<usize>) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("Local path does not exist: {}", path.display());
    }

    if !path.is_dir() {
        anyhow::bail!("Local path is not a directory: {}", path.display());
    }

    println!("nozzleup: Building from local path: {}", path.display());

    let config = Config::new()?;
    let version_label = "local".to_string();

    build_and_install(&config, path, &version_label, jobs)?;

    Ok(())
}

/// Build and install from a remote repository
pub async fn run_repo(repo: &str, jobs: Option<usize>) -> Result<()> {
    println!("nozzleup: Building from repository: {}", repo);

    let config = Config::new()?;
    let temp_dir = tempfile::tempdir().context("Failed to create temporary directory")?;

    // Clone the specified repository
    let repo_url = format!("https://github.com/{}.git", repo);

    println!("nozzleup: Cloning {}...", repo_url);

    let status = Command::new("git")
        .args(["clone", &repo_url, temp_dir.path().to_str().unwrap()])
        .status()
        .context("Failed to execute git clone")?;

    if !status.success() {
        anyhow::bail!("Failed to clone repository {}", repo);
    }

    // Build and install
    let version_label = format!("repo-{}", repo.replace('/', "-"));
    build_and_install(&config, temp_dir.path(), &version_label, jobs)?;

    Ok(())
}

/// Clone the main repository
async fn clone_repo(config: &Config, destination: &Path, branch: Option<&str>) -> Result<()> {
    check_command_exists("git")?;

    let repo_url = format!("https://github.com/{}.git", config.repo);

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
        anyhow::bail!("Failed to clone repository");
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
    fs_err::create_dir_all(&version_dir).context("Failed to create version directory")?;

    let binary_dest = version_dir.join("nozzle");

    // Copy the binary
    fs_err::copy(&binary_source, &binary_dest).context("Failed to copy binary")?;

    // Make it executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs_err::metadata(&binary_dest)
            .context("Failed to get binary metadata")?
            .permissions();
        perms.set_mode(0o755);
        fs_err::set_permissions(&binary_dest, perms)
            .context("Failed to set executable permissions")?;
    }

    // Create symlink to active binary
    let active_path = config.active_binary_path();

    // Remove existing symlink if it exists
    if active_path.exists() || active_path.is_symlink() {
        fs_err::remove_file(&active_path).context("Failed to remove existing symlink")?;
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
