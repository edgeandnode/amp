//! Provider management commands

pub mod inspect;
pub mod list;
pub mod register;
pub mod remove;

/// Provider management subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Register a provider configuration
    #[command(after_help = include_str!("provider/register__after_help.md"))]
    Register(register::Args),

    /// Inspect a provider by its name
    #[command(after_help = include_str!("provider/inspect__after_help.md"))]
    Inspect(inspect::Args),

    /// List all providers
    #[command(alias = "list")]
    #[command(after_help = include_str!("provider/list__after_help.md"))]
    Ls(list::Args),

    /// Remove a provider by its name
    #[command(alias = "remove")]
    #[command(after_help = include_str!("provider/remove__after_help.md"))]
    Rm(remove::Args),
}

/// Execute the provider command with the given subcommand.
pub async fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::Register(args) => register::run(args).await?,
        Commands::Inspect(args) => inspect::run(args).await?,
        Commands::Ls(args) => list::run(args).await?,
        Commands::Rm(args) => remove::run(args).await?,
    }
    Ok(())
}
