use clap::Parser;
use common::tracing_helpers;
use tests::test_support::{bless, TestEnv};
use tracing::warn;

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
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_helpers::register_logger();

    match args.command {
        Command::Bless {
            dataset,
            start_block,
            end_block,
        } => {
            let test_env = TestEnv::blessed("bless_cmd").await.unwrap();
            bless(&test_env, &dataset, start_block, end_block)
                .await
                .unwrap();
            warn!("wrote new blessed dataset for {dataset}");
        }
    }
}
