//! Table revision management commands

pub mod activate;
pub mod deactivate;
pub mod get;

/// Revision management subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Activate a table revision by location ID
    #[command(after_help = include_str!("table/activate__after_help.md"))]
    Activate(activate::Args),

    /// Deactivate all revisions for a table
    #[command(after_help = include_str!("table/deactivate__after_help.md"))]
    Deactivate(deactivate::Args),

    /// Get a revision by location ID
    #[command(after_help = include_str!("table/get__after_help.md"))]
    Get(get::Args),
}

/// Execute the revision command with the given subcommand.
pub async fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::Activate(args) => activate::run(args).await?,
        Commands::Deactivate(args) => deactivate::run(args).await?,
        Commands::Get(args) => get::run(args).await?,
    }
    Ok(())
}
