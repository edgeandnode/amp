//! Table revision management commands

pub mod activate;
pub mod deactivate;
pub mod delete;
pub mod get;
pub mod list;
pub mod register;
pub mod restore;

/// Revision management subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Activate a table revision by location ID
    #[command(after_help = include_str!("table/activate__after_help.md"))]
    Activate(activate::Args),

    /// Deactivate all revisions for a table
    #[command(after_help = include_str!("table/deactivate__after_help.md"))]
    Deactivate(deactivate::Args),

    /// Delete a table revision by location ID
    #[command(alias = "rm")]
    #[command(after_help = include_str!("table/delete__after_help.md"))]
    Delete(delete::Args),

    /// Get a revision by location ID
    #[command(after_help = include_str!("table/get__after_help.md"))]
    Get(get::Args),

    /// List all table revisions
    #[command(alias = "ls")]
    #[command(after_help = include_str!("table/list__after_help.md"))]
    List(list::Args),

    /// Register an inactive table revision from a given path
    #[command(after_help = include_str!("table/register__after_help.md"))]
    Register(register::Args),

    /// Restore a revision by re-registering its files from object storage
    #[command(after_help = include_str!("table/restore__after_help.md"))]
    Restore(restore::Args),
}

/// Execute the revision command with the given subcommand.
pub async fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::Activate(args) => activate::run(args).await?,
        Commands::Deactivate(args) => deactivate::run(args).await?,
        Commands::Delete(args) => delete::run(args).await?,
        Commands::Get(args) => get::run(args).await?,
        Commands::List(args) => list::run(args).await?,
        Commands::Register(args) => register::run(args).await?,
        Commands::Restore(args) => restore::run(args).await?,
    }
    Ok(())
}
