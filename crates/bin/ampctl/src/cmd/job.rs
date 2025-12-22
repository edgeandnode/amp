//! Job management commands

pub mod inspect;
pub mod list;
pub mod prune;
pub mod resume;
pub mod rm;
pub mod stop;

/// Job management subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// List jobs with pagination
    #[command(alias = "ls")]
    #[command(after_help = include_str!("job/list__after_help.md"))]
    List(list::Args),

    /// Inspect detailed information about a job
    #[command(alias = "get")]
    #[command(after_help = include_str!("job/inspect__after_help.md"))]
    Inspect(inspect::Args),

    /// Stop a running job
    #[command(after_help = include_str!("job/stop__after_help.md"))]
    Stop(stop::Args),

    /// Resume a stopped job
    #[command(after_help = include_str!("job/resume__after_help.md"))]
    Resume(resume::Args),

    /// Remove job(s) by identifier or status filter
    #[command(alias = "remove")]
    #[command(after_help = include_str!("job/rm__after_help.md"))]
    Rm(rm::Args),

    /// Prune jobs by status filter
    #[command(after_help = include_str!("job/prune__after_help.md"))]
    Prune(prune::Args),
}

/// Execute the job command with the given subcommand.
pub async fn run(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::List(args) => list::run(args).await?,
        Commands::Inspect(args) => inspect::run(args).await?,
        Commands::Stop(args) => stop::run(args).await?,
        Commands::Resume(args) => resume::run(args).await?,
        Commands::Rm(args) => rm::run(args).await?,
        Commands::Prune(args) => prune::run(args).await?,
    }
    Ok(())
}
