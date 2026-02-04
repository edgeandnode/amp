//! Dataset build command.
//!
//! Builds a derived dataset from `amp.yaml` configuration by:
//! 1. Parsing the `amp.yaml` configuration file
//! 2. Discovering tables from the `tables/` directory
//! 3. Resolving dependencies from the admin API
//! 4. Rendering Jinja SQL templates with context
//! 5. Validating SELECT statements and incremental constraints
//! 6. Inferring table schemas via DataFusion planning
//! 7. Writing output files to the output directory:
//!    - `manifest.json` (canonical)
//!    - `tables/<table>.sql` (rendered SQL)
//!    - `tables/<table>.ipc` (inferred schema)
//!    - `functions/` (if any)
//! 8. Updating or creating `amp.lock` lockfile
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use std::{
    fs,
    path::{Path, PathBuf},
};

use dataset_authoring::{
    arrow_json,
    cache::Cache,
    config::{self, AmpYaml},
    lockfile::{Lockfile, LockfileError, RootInfo},
    manifest::ManifestBuilder,
    resolver::{DependencyGraph, Resolver},
    validation::{self, ResolutionMode, ValidationOptions},
};
use datasets_common::table_name::TableName;

use super::authoring;
use crate::args::GlobalArgs;

/// Command-line arguments for the `dataset build` command.
#[derive(Debug, clap::Args)]
#[command(
    about = "Build a dataset from amp.yaml configuration",
    after_help = include_str!("build__after_help.md")
)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Directory containing amp.yaml (defaults to current directory).
    #[arg(long, short = 'd', default_value = ".")]
    pub dir: PathBuf,

    /// Output directory for build artifacts (required).
    #[arg(long, short = 'o', required = true)]
    pub output: PathBuf,

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

/// Result of a dataset build operation.
#[derive(serde::Serialize)]
struct BuildResult {
    status: &'static str,
    output: String,
    manifest_hash: String,
    tables: usize,
    functions: usize,
}

impl std::fmt::Display for BuildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Build complete")?;
        writeln!(f, "  Output: {}", self.output)?;
        writeln!(f, "  Manifest hash: {}", self.manifest_hash)?;
        writeln!(f, "  Tables: {}", self.tables)?;
        if self.functions > 0 {
            writeln!(f, "  Functions: {}", self.functions)?;
        }
        Ok(())
    }
}

/// Build a dataset from amp.yaml configuration.
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
        output,
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
        config,
        discovered_tables,
        dependencies: dep_graph,
        validated_tables,
        ..
    } = validation_output;

    tracing::debug!(
        namespace = %config.namespace,
        name = %config.name,
        version = %config.version,
        tables = discovered_tables.len(),
        "Loaded configuration"
    );
    tracing::debug!(
        direct_deps = dep_graph.direct.len(),
        transitive_deps = dep_graph.len(),
        "Resolved dependencies"
    );

    // 5. Handle lockfile
    if !locked {
        handle_lockfile(&lockfile_path, &config, &dep_graph)?;
    }

    // 6. Create output directories
    let sql_dir = output.join("sql");
    let functions_dir = output.join("functions");
    fs::create_dir_all(&sql_dir).map_err(|e| Error::CreateDirectory {
        path: sql_dir.clone(),
        source: e,
    })?;

    // 7. Write table artifacts
    tracing::info!("Writing table artifacts");
    for (table_name, table) in &validated_tables {
        let sql_output_path = sql_dir.join(format!("{table_name}.sql"));
        fs::write(&sql_output_path, &table.rendered_sql).map_err(|e| Error::WriteFile {
            path: sql_output_path,
            source: e,
        })?;

        let schema_output_path = sql_dir.join(format!("{table_name}.schema.json"));
        arrow_json::write_schema_file(&table.schema.arrow, &schema_output_path).map_err(|e| {
            Error::WriteSchemaFile {
                table: table_name.clone(),
                source: e,
            }
        })?;
    }

    // 8. Copy function files if present
    if let Some(functions) = &config.functions {
        fs::create_dir_all(&functions_dir).map_err(|e| Error::CreateDirectory {
            path: functions_dir.clone(),
            source: e,
        })?;

        for (func_name, func_def) in functions {
            let source_path = dir.join(&func_def.source);
            let dest_path = functions_dir.join(format!("{}.js", func_name));
            fs::copy(&source_path, &dest_path).map_err(|e| Error::CopyFile {
                source: source_path,
                dest: dest_path,
                error: e,
            })?;
        }
    }

    // 9. Generate manifest
    tracing::info!("Generating manifest");
    let manifest = ManifestBuilder::new(&config, &dep_graph, &output, &sql_dir, &discovered_tables)
        .build()
        .map_err(Error::ManifestGeneration)?;

    // Write manifest.json (canonical JSON for identity verification)
    let manifest_json = manifest
        .canonical_json()
        .map_err(Error::ManifestGeneration)?;
    let manifest_path = output.join("manifest.json");
    fs::write(&manifest_path, &manifest_json).map_err(|e| Error::WriteFile {
        path: manifest_path,
        source: e,
    })?;

    let identity = manifest.identity().map_err(Error::ManifestGeneration)?;
    tracing::info!(hash = %identity, "Build complete");

    let functions = config
        .functions
        .as_ref()
        .map(|funcs| funcs.len())
        .unwrap_or(0);
    let result = BuildResult {
        status: "ok",
        output: output.display().to_string(),
        manifest_hash: identity.to_string(),
        tables: discovered_tables.len(),
        functions,
    };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

fn handle_lockfile(
    lockfile_path: &Path,
    config: &AmpYaml,
    dep_graph: &DependencyGraph,
) -> Result<(), Error> {
    let root = RootInfo {
        namespace: config.namespace.clone(),
        name: config.name.clone(),
        version: config.version.clone(),
    };

    // Update lockfile if deps have changed
    let new_lockfile = Lockfile::from_graph(root, dep_graph);
    let should_write = if lockfile_path.exists() {
        let existing = Lockfile::read(lockfile_path).map_err(Error::Lockfile)?;
        existing != new_lockfile
    } else {
        true
    };
    if should_write {
        new_lockfile.write(lockfile_path).map_err(Error::Lockfile)?;
        tracing::info!("Updated amp.lock");
    }

    Ok(())
}

/// Errors for dataset build operations.
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

    /// Failed to create directory.
    #[error("failed to create directory {}", path.display())]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Failed to write file.
    #[error("failed to write file {}", path.display())]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Failed to write schema file.
    #[error("failed to write schema for table '{table}'")]
    WriteSchemaFile {
        table: TableName,
        #[source]
        source: dataset_authoring::arrow_json::SchemaFileError,
    },

    /// Failed to copy function file.
    #[error("failed to copy function file from {} to {}", source.display(), dest.display())]
    CopyFile {
        source: PathBuf,
        dest: PathBuf,
        #[source]
        error: std::io::Error,
    },

    /// Manifest generation failed.
    #[error("manifest generation failed")]
    ManifestGeneration(#[source] dataset_authoring::manifest::ManifestError),

    /// Failed to format build JSON.
    #[error("failed to format build JSON")]
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
