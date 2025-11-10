use std::path::PathBuf;

use anyhow::Result;

use crate::{
    DEFAULT_REPO_PRIVATE,
    builder::{BuildOptions, BuildSource, Builder},
    config::Config,
    ui,
    version_manager::VersionManager,
};

/// Main entry point for build command - handles all build source combinations
#[expect(clippy::too_many_arguments)]
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
            repo: DEFAULT_REPO_PRIVATE.to_string(),
            branch,
        },
        (None, None, None, Some(commit), None) => BuildSource::Commit {
            repo: DEFAULT_REPO_PRIVATE.to_string(),
            commit,
        },
        (None, None, None, None, Some(number)) => BuildSource::Pr {
            repo: DEFAULT_REPO_PRIVATE.to_string(),
            number,
        },
        (None, None, None, None, None) => BuildSource::Main {
            repo: DEFAULT_REPO_PRIVATE.to_string(),
        },
        _ => unreachable!("Clap should prevent conflicting options"),
    };

    ui::info!("Building from source: {}", source);

    // Create builder
    let config = Config::new(install_dir)?;
    let version_manager = VersionManager::new(config);
    let builder = Builder::new(version_manager);

    // Execute the build
    builder.build(source, BuildOptions { name, jobs }).await?;

    Ok(())
}
