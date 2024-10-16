use clap::Parser;
use log::warn;
use tests::test_support::bless;

/// CLI for test support.
#[derive(Parser, Debug)]
#[command(name = "tests")]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Take a snapshot of a dataset and make it the blessed one.
    Bless {
        /// Name of the dataset to dump.
        dataset: String,

        /// Start block number.
        start_block: u64,

        /// End block number.
        end_block: u64,
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.command {
        Commands::Bless {
            dataset,
            start_block,
            end_block,
        } => {
            bless(&dataset, start_block, end_block).await.unwrap();
            warn!("wrote new blessed dataset for {dataset}");
        }
    }
}
