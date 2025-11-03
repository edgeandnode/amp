//! Dataset management commands

pub mod inspect;
pub mod list;
pub mod manifest;
pub mod register;
pub mod versions;

/// Dataset management subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Register a dataset manifest
    #[command(alias = "reg")]
    #[command(after_help = include_str!("dataset/register__after_help.md"))]
    Register(register::Args),

    /// List all registered datasets
    #[command(alias = "ls")]
    #[command(after_help = include_str!("dataset/list__after_help.md"))]
    List(list::Args),

    /// Inspect detailed information about a dataset
    #[command(alias = "get")]
    #[command(after_help = include_str!("dataset/inspect__after_help.md"))]
    Inspect(inspect::Args),

    /// List all versions of a dataset
    #[command(after_help = include_str!("dataset/versions__after_help.md"))]
    Versions(versions::Args),

    /// Get the manifest JSON for a dataset
    #[command(after_help = include_str!("dataset/manifest__after_help.md"))]
    Manifest(manifest::Args),
}

/// Execute the dataset command with the given subcommand.
pub async fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::Register(args) => register::run(args).await?,
        Commands::List(args) => list::run(args).await?,
        Commands::Inspect(args) => inspect::run(args).await?,
        Commands::Versions(args) => versions::run(args).await?,
        Commands::Manifest(args) => manifest::run(args).await?,
    }
    Ok(())
}
