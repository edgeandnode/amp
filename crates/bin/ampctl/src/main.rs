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
    /// Generate a dataset manifest file
    ///
    /// Creates a dataset manifest for supported dataset kinds (evm-rpc, eth-beacon,
    /// firehose). The manifest can be written to a file or printed to stdout.
    ///
    /// If the output is a directory, the filename will match the dataset kind.
    /// If no output is specified, the manifest will be printed to stdout.
    GenManifest(cmd::gen_manifest::Args),

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

    /// Register a provider configuration with the engine admin interface
    ///
    /// Loads a provider configuration from local or remote storage and registers it
    /// with the Amp engine admin interface. The provider configuration defines
    /// connection details for external data sources (EVM RPC endpoints, Firehose, etc.).
    ///
    /// Supports local filesystem and object storage (s3://, gs://, az://, file://).
    /// The engine admin interface validates the provider configuration and makes it
    /// available for use by datasets.
    #[command(after_help = include_str!("cmd/reg_provider__after_help.md"))]
    RegProvider(cmd::reg_provider::Args),
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::GenManifest(args) => cmd::gen_manifest::run(args).await?,
        Commands::RegManifest(args) => cmd::reg_manifest::run(args).await?,
        Commands::RegProvider(args) => cmd::reg_provider::run(args).await?,
    }

    Ok(())
}
