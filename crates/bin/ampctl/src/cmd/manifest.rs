//! Manifest management commands

pub mod inspect;
pub mod register;
pub mod remove;

/// Manifest management subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Register a manifest with content-addressable storage
    #[command(after_help = include_str!("manifest/register__after_help.md"))]
    Register(register::Args),

    /// Inspect a manifest by its hash
    #[command(after_help = include_str!("manifest/inspect__after_help.md"))]
    Inspect(inspect::Args),

    /// Remove a manifest by its hash
    #[command(alias = "remove")]
    #[command(after_help = include_str!("manifest/remove__after_help.md"))]
    Rm(remove::Args),
}

/// Execute the manifest command with the given subcommand.
pub async fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::Register(args) => register::run(args).await?,
        Commands::Inspect(args) => inspect::run(args).await?,
        Commands::Rm(args) => remove::run(args).await?,
    }
    Ok(())
}
