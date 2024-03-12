use arrow::compute::concat_batches;
use clap::Parser;
use datafusion::arrow;
use datafusion::parquet;
use firehose_datasources::client::Client;
use firehose_datasources::evm::pbethereum;
use firehose_datasources::evm::protobufs_to_rows;
use firehose_datasources::evm::tables;
use fs_err::{self as fs, OpenOptions};
use futures::StreamExt as _;
use parquet::arrow::ArrowWriter as ParquetWriter;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
use std::sync::Arc;
use std::{collections::HashMap, io::Write as _};

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

    if end == 0 {
        return Err(anyhow::anyhow!(
            "The end block number must be greater than 0"
        ));
    }

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
        let file_path = out_dir.join(format!("pb_blocks_{start}_to_{end}.json"));
        Some(file_writer(&file_path)?)
    } else {
        None
    };

    // Watch https://github.com/apache/arrow-datafusion/issues/9493 for speed-ups to parquet writing.
    // Maps table names to parquet writers.
    let mut parquet_writers: Option<HashMap<String, ParquetWriter<_>>> = if parquet {
        let writer = firehose_datasources::evm::tables::all_tables()
            .into_iter()
            .map(|table| -> Result<_, anyhow::Error> {
                let name = &table.name;
                let table_dir = out_dir.join(name);
                if !table_dir.exists() {
                    fs::create_dir(&table_dir)?;
                }
                let file_path = table_dir.join(format!("{start}_to_{end}.parquet"));
                let file_writer = file_writer(&file_path)?;
                let schema = table.schema.into();
                Ok((
                    table.name,
                    ParquetWriter::try_new(file_writer, schema, Some(parquet_options()))?,
                ))
            })
            .collect::<Result<_, _>>()?;
        Some(writer)
    } else {
        None
    };

    let mut stream = Box::pin(client.blocks(args.start, args.end).await?);

    // Polls the stream concurrently to the main task
    let (tx, mut block_stream) = tokio::sync::mpsc::channel(100);
    tokio::spawn(async move {
        while let Some(block) = stream.next().await {
            let _ = tx.send(block).await;
        }
    });
    while let Some(block) = block_stream.recv().await {
        let block = block?;
        if block.number % 100000 == 0 {
            println!("Reached block {}", block.number);
        }

        if let Some(pb_writer) = &mut pb_writer {
            // This is writing each block as a separate JSON file.
            write_block_to_pb_json(pb_writer, &block)?;
        }

        if let Some(parquet_writers) = &mut parquet_writers {
            use tables::{blocks, calls, logs, transactions};

            let (block, transactions, calls, logs) = protobufs_to_rows(block)?;

            let block = block.to_arrow()?;

            let transactions_batch = {
                let mut batches = vec![];
                for tx in transactions {
                    batches.push(tx.to_arrow()?);
                }
                let schema = &Arc::new(transactions::schema());
                concat_batches(schema, batches.iter())?
            };

            let calls_batch = {
                let mut batches = vec![];
                for call in calls {
                    batches.push(call.to_arrow()?);
                }
                let schema = &Arc::new(calls::schema());
                concat_batches(schema, batches.iter())?
            };

            let logs_batch = {
                let mut batches = vec![];
                for log in logs {
                    batches.push(log.to_arrow()?);
                }
                let schema = &Arc::new(logs::schema());
                concat_batches(schema, batches.iter())?
            };

            let blocks_writer = parquet_writers.get_mut(blocks::TABLE_NAME).unwrap();
            blocks_writer.write(&block)?;

            let transactions_writer = parquet_writers.get_mut(transactions::TABLE_NAME).unwrap();
            transactions_writer.write(&transactions_batch)?;

            let calls_writer = parquet_writers.get_mut(calls::TABLE_NAME).unwrap();
            calls_writer.write(&calls_batch)?;

            let logs_writer = parquet_writers.get_mut(logs::TABLE_NAME).unwrap();
            logs_writer.write(&logs_batch)?;
        }
    }

    if let Some(mut pb_writer) = pb_writer {
        pb_writer.flush()?;
    }

    if let Some(parquet_writers) = parquet_writers {
        for (_, writer) in parquet_writers {
            writer.close()?;
        }
    }

    Ok(())
}

fn parquet_options() -> ParquetWriterProperties {
    use parquet::basic::{Compression, ZstdLevel};

    // For DataFusion defaults, see `ParquetOptions` here:
    // https://github.com/apache/arrow-datafusion/blob/main/datafusion/common/src/config.rs
    let compression = Compression::ZSTD(ZstdLevel::try_new(1).unwrap());

    // Note: We could set `sorting_columns` for columns like `block_number` and `ordinal`. However,
    // Datafusion doesn't actually read that metadata info anywhere and just reiles on the
    // `file_sort_order` set on the reader configuration.
    ParquetWriterProperties::builder()
        .set_compression(compression)
        .build()
}

fn file_writer(path: &std::path::Path) -> Result<fs::File, anyhow::Error> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(path)?;
    file.set_len(0)?;
    Ok(file)
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
