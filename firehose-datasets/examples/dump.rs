use clap::Parser;
use datafusion::datasource::file_format::parquet;
use firehose_datasets::client::Client;
use firehose_datasets::evm::pbethereum;
use fs_err::{self as fs, OpenOptions};
use futures::StreamExt as _;
use std::io::{BufWriter, Write as _};

/// A tool for dumping a range of firehose blocks to a protobufs json file and/or for converting them
/// to parquet tables.
#[derive(Parser, Debug)]
#[command(name = "firehose-dump")]
struct Args {
    /// Path to a provider config file. Example config:
    ///
    /// ```toml
    /// url = "http://localhost:8080"
    /// token = "secret"
    /// ```
    #[arg(long, short, env = "DUMP_FIREHOSE_PROVIDER")]
    config: String,

    /// The block number to start from, inclusive.
    start: u64,

    /// The block number to end at, inclusive.
    end: u64,

    /// The directory to write the output files to.
    #[arg(long, short)]
    out: String,

    /// Whether to dump to protobufs json file. The whole range will be dumped into a single file,
    /// containing one JSON object per block.
    #[arg(long)]
    pb_json: bool,

    /// Whether to convert to parquet tables.
    #[arg(long)]
    parquet: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let Args {
        config,
        start,
        end,
        out,
        pb_json,
        parquet,
    } = args;

    let mut client = {
        let config = fs::read_to_string(&config)?;
        let provider = toml::from_str(&config)?;
        Client::new(provider).await?
    };

    let out_dir = std::path::Path::new(&out);
    if !out_dir.exists() {
        fs::create_dir(out_dir)?;
    }
    let mut pb_writer = if pb_json {
        let pb_file_path = out_dir.join(format!("pb_blocks_{start}_to_{end}.json"));
        let pb_file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(pb_file_path)?;
        pb_file.set_len(0)?;
        Some(BufWriter::new(pb_file))
    } else {
        None
    };

    // let arrow_builder = if parquet { serde_arrow::ArrowBuilder::new(} else { None };
    let mut stream = Box::pin(client.blocks(args.start, args.end).await?);
    while let Some(block) = stream.next().await {
        let block = block?;

        if let Some(pb_writer) = &mut pb_writer {
            // This is writing each block as a separate JSON file.
            write_block_to_pb_json(pb_writer, &block)?;
        }
    }

    if let Some(mut pb_writer) = pb_writer {
        pb_writer.flush()?;
    }

    Ok(())
}

fn write_block_to_pb_json(
    writer: &mut impl std::io::Write,
    block: &pbethereum::Block,
) -> Result<(), anyhow::Error> {
    // This is to get a hex representation for bytes arrays in the JSON.
    let mut json_block = serde_json::to_value(&block)?;
    replace_u8_arrays_with_hex_string(&mut json_block);

    serde_json::to_writer_pretty(writer, &json_block)?;
    Ok(())
}

fn replace_u8_arrays_with_hex_string(value: &mut serde_json::Value) {
    use serde_json::Value;

    fn is_u8(v: &Value) -> bool {
        use Value::Number;
        match v {
            Number(num) => num.as_u64().map_or(false, |u| u <= u8::MAX as u64),
            _ => false, // Not a number
        }
    }

    match value {
        Value::Object(map) => {
            for (_, v) in map {
                replace_u8_arrays_with_hex_string(v);
            }
        }
        Value::Array(vec) => {
            // 32 is hashes, 20 is addresses, 256 is logs bloom.
            if (vec.len() == 32 || vec.len() == 20 || vec.len() == 256) && vec.iter().all(is_u8) {
                // Convert the 32-byte array to a hex string.
                let bytes: Vec<u8> = vec.iter().map(|v| v.as_u64().unwrap() as u8).collect();
                let hex_str = format!("0x{}", hex::encode(bytes));
                *value = Value::String(hex_str);
            } else {
                for v in vec {
                    replace_u8_arrays_with_hex_string(v);
                }
            }
        }
        _ => {}
    }
}
