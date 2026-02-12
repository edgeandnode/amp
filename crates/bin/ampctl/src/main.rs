use ampctl::cmd;
use clap::Parser as _;

#[tokio::main]
async fn main() {
    // Install rustls crypto provider before any TLS operations.
    // Required when both ring and aws-lc-rs are available in the dependency tree.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install default crypto provider");

    // Initialize tracing for debug logs
    let _ = monitoring::init(None);

    if let Err(err) = run().await {
        ampctl::error!(err);
        std::process::exit(1);
    }
}

/// Control plane CLI for Amp
#[derive(Debug, clap::Parser)]
#[command(name = "ampctl")]
#[command(about = "ampctl controls the Amp engine and infrastructure")]
#[command(long_about = include_str!("cmd/root__long_about.md"))]
#[command(after_long_help = include_str!("cmd/root__after_long_help.md"))]
#[command(version = env!("VERGEN_GIT_DESCRIBE"))]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// Manage manifests in content-addressable storage
    #[command(subcommand)]
    #[command(alias = "manifests")]
    #[command(long_about = include_str!("cmd/manifest__long_about.md"))]
    Manifest(cmd::manifest::Commands),

    /// Manage provider configurations
    #[command(subcommand)]
    #[command(alias = "providers")]
    #[command(long_about = include_str!("cmd/provider__long_about.md"))]
    Provider(cmd::provider::Commands),

    /// Manage extraction and deployment jobs
    #[command(subcommand)]
    #[command(alias = "jobs")]
    #[command(long_about = include_str!("cmd/job__long_about.md"))]
    Job(cmd::job::Commands),

    /// Manage datasets
    #[command(subcommand)]
    #[command(alias = "datasets")]
    #[command(long_about = include_str!("cmd/dataset__long_about.md"))]
    Dataset(cmd::dataset::Commands),

    /// Manage table revisions
    #[command(subcommand)]
    #[command(alias = "tables")]
    #[command(long_about = include_str!("cmd/table__long_about.md"))]
    Table(cmd::table::Commands),

    /// Manage worker nodes
    #[command(subcommand)]
    #[command(alias = "workers")]
    #[command(long_about = include_str!("cmd/worker__long_about.md"))]
    Worker(cmd::worker::Commands),

    /// Verify dataset integrity
    #[command()]
    Verify(verification::Args),
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Manifest(command) => cmd::manifest::run(command).await?,
        Commands::Provider(command) => cmd::provider::run(command).await?,
        Commands::Job(command) => cmd::job::run(command).await?,
        Commands::Dataset(command) => cmd::dataset::run(command).await?,
        Commands::Table(command) => cmd::table::run(command).await?,
        Commands::Worker(command) => cmd::worker::run(command).await?,
        Commands::Verify(args) => verification::run(args).await?,
    }

    Ok(())
}
