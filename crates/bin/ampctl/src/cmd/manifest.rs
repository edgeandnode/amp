//! Manifest management commands

pub mod generate;
pub mod inspect;
pub mod list;
pub mod prune;
pub mod register;
pub mod remove;

/// Manifest management subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Generate a dataset manifest file
    ///
    /// Creates a dataset manifest for supported dataset kinds (evm-rpc, eth-beacon,
    /// firehose). The manifest can be written to a file or printed to stdout.
    ///
    /// If the output is a directory, the filename will match the dataset kind.
    /// If no output is specified, the manifest will be printed to stdout.
    Generate(generate::Args),

    /// Register a manifest with content-addressable storage
    #[command(after_help = include_str!("manifest/register__after_help.md"))]
    Register(register::Args),

    /// List all registered manifests
    #[command(after_help = include_str!("manifest/list__after_help.md"))]
    List(list::Args),

    /// Inspect a manifest by its hash
    #[command(alias = "get")]
    #[command(after_help = include_str!("manifest/inspect__after_help.md"))]
    Inspect(inspect::Args),

    /// Remove a manifest by its hash
    #[command(alias = "remove")]
    #[command(after_help = include_str!("manifest/remove__after_help.md"))]
    Rm(remove::Args),

    /// Prune all orphaned manifests (not linked to any datasets)
    #[command(after_help = include_str!("manifest/prune__after_help.md"))]
    Prune(prune::Args),
}

/// Execute the manifest command with the given subcommand.
pub async fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::Generate(args) => generate::run(args).await?,
        Commands::Register(args) => register::run(args).await?,
        Commands::List(args) => list::run(args).await?,
        Commands::Inspect(args) => inspect::run(args).await?,
        Commands::Rm(args) => remove::run(args).await?,
        Commands::Prune(args) => prune::run(args).await?,
    }
    Ok(())
}
