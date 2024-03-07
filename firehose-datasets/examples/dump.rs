use clap::Parser;
use firehose_datasets::client::Client;
use futures::StreamExt as _;
use prost::Message as _;
use std::{
    fs::{self, OpenOptions},
    io::{BufWriter, Write as _},
};

/// A tool for dumping a range of firehose blocks to protobufs files and for converting them to parquet tables
#[derive(Parser, Debug)]
#[command(name = "firehose-dump")]
struct Args {
    /// Path to a provider config file. Example config:
    ///
    /// ```toml
    /// url = "http://localhost:8080"
    /// token = "secret"
    /// ```
    ///
    /// Defaults value is "local/provider.toml", with `.local` being conveniently `.gitignore`d.
    #[arg(long, short, default_value = "local/provider.toml")]
    config: String,

    /// The block number to start from, inclusive.
    start: u64,

    /// The block number to end at, inclusive.
    end: u64,

    /// The directory to write the output files to.
    #[arg(long, short)]
    out: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let Args {
        config,
        start,
        end,
        out,
    } = args;

    let mut client = {
        let provider = toml::from_str(&std::fs::read_to_string(config)?)?;
        Client::new(provider).await?
    };

    let out_dir = std::path::Path::new(&out);
    if !out_dir.exists() {
        fs::create_dir(out_dir)?;
    }
    let mut pb_file = {
        let pb_file_path = out_dir.join(format!("blocks_{start}_to_{end}.pb"));
        let pb_file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(pb_file_path)?;
        pb_file.set_len(0)?;
        BufWriter::new(pb_file)
    };

    let mut stream = Box::pin(client.blocks(args.start, args.end).await?);
    while let Some(block) = stream.next().await {
        let block = block?;
        let bytes = block.encode_length_delimited_to_vec();
        pb_file.write_all(&bytes)?;
    }
    pb_file.flush()?;

    Ok(())
}
