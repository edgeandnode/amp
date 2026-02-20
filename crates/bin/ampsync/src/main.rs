use ampsync::{
    commands,
    config::{Cli, Command},
};
use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider before any TLS operations.
    // Required when both ring and aws-lc-rs are available in the dependency tree.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install default crypto provider");

    monitoring::logging::init();

    let cli = Cli::parse();

    match cli.command {
        Command::Sync(config) => commands::sync::run(config).await,
    }
}
