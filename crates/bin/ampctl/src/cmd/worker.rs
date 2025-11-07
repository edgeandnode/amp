//! Worker management commands

pub mod inspect;
pub mod list;

/// Worker management subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// List all workers
    #[command(alias = "ls")]
    #[command(after_help = include_str!("worker/list__after_help.md"))]
    List(list::Args),

    /// Inspect detailed information about a worker
    #[command(alias = "get")]
    #[command(after_help = include_str!("worker/inspect__after_help.md"))]
    Inspect(inspect::Args),
}

/// Execute the worker command with the given subcommand.
pub async fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::List(args) => list::run(args).await?,
        Commands::Inspect(args) => inspect::run(args).await?,
    }
    Ok(())
}
