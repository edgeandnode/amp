use clap::Parser;
use log::warn;
use tests::test_support::{bless, bless_sql_snapshots};

/// CLI for test support.
#[derive(Parser, Debug)]
#[command(name = "tests")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
enum Command {
    /// Take a snapshot of a dataset and make it the blessed one.
    Bless {
        /// Name of the dataset to dump.
        dataset: String,

        /// Start block number.
        start_block: u64,

        /// End block number.
        end_block: u64,
    },
    /// Update the snapshots of some SQL queries that we do tests against.
    BlessSqlSnapshots,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.command {
        Command::Bless {
            dataset,
            start_block,
            end_block,
        } => {
            bless(&dataset, start_block, end_block).await.unwrap();
            warn!("wrote new blessed dataset for {dataset}");
        }
        Command::BlessSqlSnapshots => {
            bless_sql_snapshots().await.unwrap();
            warn!("wrote new blessed sql snapshots");
        }
    }
}
