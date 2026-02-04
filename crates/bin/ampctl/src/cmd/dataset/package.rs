//! Dataset package command.
//!
//! Creates a deterministic `.tgz` archive from built dataset artifacts.
//! The package can be used for distribution, deployment, or as input
//! to the `dataset register` command.
//!
//! # Package Contents
//!
//! The archive contains:
//! - `manifest.json` - Canonical manifest with file references
//! - `tables/<table>.sql` - Rendered SQL files
//! - `tables/<table>.ipc` - Inferred Arrow schemas (IPC format)
//! - `functions/<name>.js` - Function source files (if any)
//!
//! # Determinism
//!
//! Archives are deterministic for identical inputs:
//! - Entries sorted alphabetically by path
//! - Fixed timestamps (Unix epoch)
//! - Consistent permissions (0o644)
//! - No ownership metadata

use std::path::PathBuf;

use dataset_authoring::package::PackageBuilder;

use crate::args::GlobalArgs;

/// Command-line arguments for the `dataset package` command.
#[derive(Debug, clap::Args)]
#[command(
    about = "Package built dataset artifacts into a .tgz archive",
    after_help = include_str!("package__after_help.md")
)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Directory containing build artifacts (with manifest.json, tables/, etc.).
    ///
    /// This should be the output directory from `ampctl dataset build`.
    /// If not specified, uses the current directory if it contains a manifest.json.
    #[arg(long, short = 'd')]
    pub dir: Option<PathBuf>,

    /// Output path for the package archive.
    ///
    /// Defaults to `dataset.tgz` in the current directory.
    #[arg(long, short = 'o', default_value = "dataset.tgz")]
    pub output: PathBuf,
}

/// Result of a dataset package operation.
#[derive(serde::Serialize)]
struct PackageResult {
    status: &'static str,
    output: String,
    hash: String,
    entries: usize,
}

impl std::fmt::Display for PackageResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Package created: {}", self.output)?;
        writeln!(f, "  SHA-256: {}", self.hash)?;
        writeln!(f, "  Entries: {}", self.entries)?;
        Ok(())
    }
}

/// Package built dataset artifacts into a distributable archive.
///
/// # Errors
///
/// Returns [`Error`] for missing files, I/O failures, or archive errors.
#[tracing::instrument(skip_all, fields(output = %args.output.display()))]
pub async fn run(args: Args) -> Result<(), Error> {
    let Args {
        global,
        dir,
        output,
    } = args;

    // Determine the directory to use
    let dir = match dir {
        Some(d) => d,
        None => {
            // Auto-detect: use CWD if it contains manifest.json
            let cwd = std::env::current_dir().map_err(Error::CurrentDir)?;
            let manifest_path = cwd.join("manifest.json");
            if !manifest_path.exists() {
                return Err(Error::MissingDirectory);
            }
            tracing::debug!(cwd = %cwd.display(), "Auto-detected package directory from CWD");
            cwd
        }
    };

    // Validate input directory exists
    if !dir.is_dir() {
        return Err(Error::InvalidDirectory { path: dir });
    }

    // Build the package from directory
    tracing::info!("Creating package from {}", dir.display());
    let builder = PackageBuilder::from_directory(&dir).map_err(Error::Package)?;

    // Log what's being included
    let entries = builder.entries();
    let entries_len = entries.len();
    tracing::debug!(entries = entries.len(), "Collected package entries");

    // Write the archive
    builder.write_to(&output).map_err(Error::Package)?;

    // Compute hash for verification
    let hash = builder.hash().map_err(Error::Package)?;

    tracing::info!(path = %output.display(), hash = %hash, "Package created");

    let result = PackageResult {
        status: "ok",
        output: output.display().to_string(),
        hash: hash.to_string(),
        entries: entries_len,
    };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    // List files if verbose logging is enabled
    if tracing::enabled!(tracing::Level::DEBUG) {
        for entry in entries {
            tracing::debug!("  - {} ({} bytes)", entry.path, entry.contents.len());
        }
    }

    Ok(())
}

/// Errors for dataset package operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to get current working directory.
    #[error("failed to get current directory")]
    CurrentDir(#[source] std::io::Error),

    /// No --dir provided and no manifest.json in current directory.
    #[error("no --dir provided and no manifest.json in current directory")]
    MissingDirectory,

    /// Invalid directory specified.
    #[error("not a directory: {}", path.display())]
    InvalidDirectory { path: PathBuf },

    /// Package assembly failed.
    #[error("package assembly failed")]
    Package(#[source] dataset_authoring::package::PackageError),

    /// Failed to format JSON for display.
    #[error("failed to format package JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
