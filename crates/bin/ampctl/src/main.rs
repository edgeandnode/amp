use ampctl::cmd;
use clap::Parser as _;

#[tokio::main]
async fn main() {
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
    /// Register a dataset manifest with the engine admin interface
    ///
    /// Loads a dataset manifest from local or remote storage and registers it
    /// with the Amp engine admin interface. The manifest defines dataset schema,
    /// source configuration, and transformation logic.
    ///
    /// Supports local filesystem and object storage (s3://, gs://, az://, file://).
    /// The engine admin interface validates the manifest and makes it available
    /// for use by the Amp infrastructure.
    #[command(after_help = include_str!("cmd/reg_manifest__after_help.md"))]
    RegManifest(cmd::reg_manifest::Args),

    /// Deploy a dataset to start syncing blockchain data
    ///
    /// Deploys a dataset version by scheduling a data extraction job via the
    /// engine admin interface. Once deployed, the dataset begins syncing blockchain
    /// data from the configured provider and storing it as Parquet files.
    ///
    /// The end block can be configured to control when syncing stops:
    /// continuous (default), latest block, specific block number, or relative to chain tip.
    #[command(after_help = include_str!("cmd/dep_dataset__after_help.md"))]
    DepDataset(cmd::dep_dataset::Args),

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
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::RegManifest(args) => cmd::reg_manifest::run(args).await?,
        Commands::DepDataset(args) => cmd::dep_dataset::run(args).await?,
        Commands::Manifest(command) => cmd::manifest::run(command).await?,
        Commands::Provider(command) => cmd::provider::run(command).await?,
        Commands::Job(command) => cmd::job::run(command).await?,
    }

    Ok(())
}
