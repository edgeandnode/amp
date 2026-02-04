//! Dataset check command.
//!
//! Validates a derived dataset project without writing build artifacts by:
//! 1. Parsing the authoring configuration
//! 2. Discovering models from the `models/` directory
//! 3. Resolving dependencies from the admin API
//! 4. Rendering Jinja SQL templates with context
//! 5. Validating SELECT statements and incremental constraints
//! 6. Inferring table schemas via DataFusion planning

use std::path::PathBuf;

use dataset_authoring::{
    cache::Cache,
    config,
    lockfile::{Lockfile, LockfileError},
    resolver::Resolver,
    validation::{self, ResolutionMode, ValidationOptions},
};

use super::authoring;
use crate::args::GlobalArgs;

/// Command-line arguments for the `dataset check` command.
#[derive(Debug, clap::Args)]
#[command(
    about = "Validate dataset authoring inputs without building artifacts",
    after_help = include_str!("check__after_help.md")
)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Directory containing amp.yaml/amp.yml (defaults to current directory).
    #[arg(long, short = 'd', default_value = ".")]
    pub dir: PathBuf,

    /// Variable overrides in the form NAME=VALUE.
    ///
    /// These take precedence over variables defined in amp.yaml.
    #[arg(long = "var", short = 'V', value_parser = authoring::parse_var)]
    pub vars: Vec<(String, String)>,

    /// Fail if amp.lock is missing or doesn't match resolved dependencies.
    #[arg(long)]
    pub locked: bool,

    /// Disable network fetches; requires amp.lock when version refs are present; cache misses are errors.
    #[arg(long)]
    pub offline: bool,

    /// Equivalent to --locked + --offline.
    #[arg(long)]
    pub frozen: bool,
}

/// Result of a dataset check operation.
#[derive(serde::Serialize)]
struct CheckResult {
    status: &'static str,
    config_path: String,
    namespace: String,
    name: String,
    version: String,
    models: usize,
    dependencies: DependencySummary,
    functions: usize,
}

#[derive(serde::Serialize)]
struct DependencySummary {
    direct: usize,
    transitive: usize,
}

impl std::fmt::Display for CheckResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Check complete")?;
        writeln!(f, "  Config: {}", self.config_path)?;
        writeln!(
            f,
            "  Dataset: {}/{} v{}",
            self.namespace, self.name, self.version
        )?;
        writeln!(f, "  Models: {}", self.models)?;
        writeln!(
            f,
            "  Dependencies: {} direct, {} total",
            self.dependencies.direct, self.dependencies.transitive
        )?;
        if self.functions > 0 {
            writeln!(f, "  Functions: {}", self.functions)?;
        }
        Ok(())
    }
}

/// Validate a dataset authoring project without writing build artifacts.
///
/// # Errors
///
/// Returns [`Error`] for configuration errors, resolution failures,
/// SQL validation errors, or I/O failures.
#[tracing::instrument(skip_all, fields(dir = %args.dir.display()))]
pub async fn run(args: Args) -> Result<(), Error> {
    let Args {
        global,
        dir,
        vars,
        locked,
        offline,
        frozen,
    } = args;
    let locked = locked || frozen;
    let offline = offline || frozen;

    // Ensure dir exists and is a directory
    if !dir.is_dir() {
        return Err(Error::InvalidDirectory { path: dir });
    }

    // 1. Parse amp.yaml/amp.yml (needed for lockfile mode selection)
    tracing::info!("Parsing authoring config");
    let (config_path, config) = config::load_from_dir(&dir)?;
    tracing::debug!(path = %config_path.display(), "Resolved config path");

    let lockfile_path = Lockfile::path_in(&dir);

    // 2. Setup resolver with admin API fetcher
    let cache = Cache::new().map_err(Error::CacheSetup)?;
    let fetcher = authoring::AdminApiFetcher::new(&global).map_err(Error::ClientBuildError)?;
    let resolver = Resolver::new(fetcher, cache).with_offline(offline);

    // 3. Determine resolution mode
    let lockfile = if locked {
        Some(authoring::load_lockfile_for_locked(
            &lockfile_path,
            &config,
        )?)
    } else if offline && authoring::dependencies_have_version_refs(&config.dependencies) {
        Some(authoring::load_lockfile_for_offline(&lockfile_path)?)
    } else {
        None
    };

    let resolution = match lockfile.as_ref() {
        Some(lockfile) => ResolutionMode::Locked { lockfile },
        None => ResolutionMode::Resolve,
    };

    // 4. Run shared validation pipeline
    tracing::info!("Validating dataset project");
    let validation_output = validation::validate_project_with_config(
        ValidationOptions::new(&dir, &resolver)
            .with_cli_vars(&vars)
            .with_resolution(resolution),
        config_path,
        config,
    )
    .await
    .map_err(Error::Validation)?;

    let validation::ValidationOutput {
        config_path,
        config,
        discovered_models,
        dependencies: dep_graph,
        ..
    } = validation_output;

    tracing::debug!(
        namespace = %config.namespace,
        name = %config.name,
        version = %config.version,
        models = discovered_models.len(),
        "Loaded configuration"
    );
    tracing::debug!(
        direct_deps = dep_graph.direct.len(),
        transitive_deps = dep_graph.len(),
        "Resolved dependencies"
    );

    let functions = config.functions.as_ref().map(|f| f.len()).unwrap_or(0);

    let result = CheckResult {
        status: "ok",
        config_path: config_path.display().to_string(),
        namespace: config.namespace.to_string(),
        name: config.name.to_string(),
        version: config.version.to_string(),
        models: discovered_models.len(),
        dependencies: DependencySummary {
            direct: dep_graph.direct.len(),
            transitive: dep_graph.len(),
        },
        functions,
    };

    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Errors for dataset check operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid directory specified.
    #[error("not a directory: {}", path.display())]
    InvalidDirectory { path: PathBuf },

    /// Failed to locate, read, or parse the authoring config.
    #[error(transparent)]
    ConfigLoad(#[from] dataset_authoring::config::ConfigLoadError),

    /// Failed to setup cache.
    #[error("failed to setup cache")]
    CacheSetup(#[source] dataset_authoring::cache::CacheError),

    /// Failed to build admin API client.
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Dataset validation failed.
    #[error(transparent)]
    Validation(#[from] dataset_authoring::validation::ValidationError),

    /// Lockfile required but not found (--locked mode).
    #[error("amp.lock not found (required for --locked mode)")]
    LockfileRequired,

    /// Lockfile required for offline version resolution.
    #[error(
        "offline mode requires amp.lock when version refs are present; add amp.lock, pin dependencies by hash, or run without --offline (expected at {0})"
    )]
    OfflineLockfileRequired(PathBuf),

    /// Lockfile operation failed.
    #[error("lockfile error")]
    Lockfile(#[source] LockfileError),

    /// Failed to format JSON for display.
    #[error("failed to format check JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}

impl From<authoring::LockfileLoadError> for Error {
    fn from(err: authoring::LockfileLoadError) -> Self {
        match err {
            authoring::LockfileLoadError::LockfileRequired => Error::LockfileRequired,
            authoring::LockfileLoadError::OfflineLockfileRequired(path) => {
                Error::OfflineLockfileRequired(path)
            }
            authoring::LockfileLoadError::Lockfile(source) => Error::Lockfile(source),
        }
    }
}
