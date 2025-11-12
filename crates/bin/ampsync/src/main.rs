use ampsync::{
    commands,
    config::{Cli, Command},
};
use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    monitoring::logging::init();

    let cli = Cli::parse();

    match cli.command {
        Command::Sync(config) => commands::sync::run(config).await,
    }
}
