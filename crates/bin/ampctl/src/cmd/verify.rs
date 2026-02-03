use alloy::primitives::BlockNumber;
use reqwest::Url;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long, default_value = "http://localhost:1602")]
    pub amp_url: String,

    #[arg(long, default_value = "")]
    pub amp_auth: String,

    #[arg(long, default_value = "edgeandnode/ethereum_mainnet")]
    pub dataset: String,

    #[arg(long, default_value = "0")]
    pub start_block: BlockNumber,

    #[arg(long)]
    pub end_block: BlockNumber,

    #[arg(long, default_value = "1")]
    pub parallelism: usize,

    /// Optional RPC URL for fallback verification when block verification fails.
    /// When provided, failed blocks will be fetched from this RPC endpoint and re-verified.
    #[arg(long)]
    pub rpc_url: Option<Url>,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let verification_args = verification::Args {
        amp_url: args.amp_url,
        amp_auth: args.amp_auth,
        dataset: args.dataset,
        start_block: args.start_block,
        end_block: args.end_block,
        parallelism: args.parallelism,
        rpc_url: args.rpc_url,
    };

    verification::run(verification_args).await
}
