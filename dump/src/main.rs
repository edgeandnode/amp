use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use common::config::Config;
use common::tracing;
use common::BoxError;
use datafusion::parquet;
use dataset_store::DatasetStore;
use dump::dump_dataset;
use dump::parquet_opts;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// A tool for dumping firehose or substreams data to parquet files.
#[derive(Parser, Debug)]
#[command(name = "firehose-dump")]
struct Args {
    /// Path to a config file. See README for details on the format.
    #[arg(long, env = "NOZZLE_CONFIG")]
    config: String,

    /// The name of the dataset to dump. This will be looked up in the dataset definiton directory.
    /// Will also be used as a subdirectory in the output path, `<data_dir>/<dataset>`.
    ///
    /// Also accepts a comma-separated list of datasets, which will be dumped in the provided order.
    #[arg(long, required = true, env = "DUMP_DATASET", value_delimiter = ',')]
    dataset: Vec<String>,

    /// The block number to start from, inclusive. If ommited, defaults to `0`. Note that `dump` is
    /// smart about keeping track of what blocks have already been dumped, so you only need to set
    /// this if you really don't want the data before this block.
    #[arg(long, short, default_value = "0", env = "DUMP_START_BLOCK")]
    start: u64,

    /// The block number to end at, inclusive. If starts with "+" then relative to `start`. If
    /// ommited, defaults to a recent block.
    #[arg(long, short, env = "DUMP_END_BLOCK")]
    end_block: Option<String>,

    /// How many parallel extractor jobs to run. Defaults to 1. Each job will be responsible for an
    /// equal number of blocks. Example: If start = 0, end = 10_000_000 and n_jobs = 10, then each
    /// job will be responsible for a contiguous section of 1 million blocks.
    #[arg(long, short = 'j', default_value = "1", env = "DUMP_N_JOBS")]
    n_jobs: u16,

    /// Overrides the `data_dir` in the config file.
    #[arg(long, env = "DUMP_DATA_DIR")]
    data_dir: Option<String>,

    /// The size of each partition in MB. Once the size is reached, a new part file is created. This
    /// is based on the estimated in-memory size of the data. The actual on-disk file size will vary,
    /// but will correlate with this value. Defaults to 4 GB.
    #[arg(long, default_value = "4096", env = "DUMP_PARTITION_SIZE_MB")]
    partition_size_mb: u64,

    /// Whether to disable compression when writing parquet files. Defaults to false.
    #[arg(long, env = "DUMP_DISABLE_COMPRESSION")]
    disable_compression: bool,

    /// How often to run the dump job in minutes. By default will run once and exit.
    #[arg(long, env = "DUMP_RUN_EVERY_MINS")]
    run_every_mins: Option<u64>,
}

#[tokio::main]
async fn main() {
    match main_inner().await {
        Ok(()) => {}
        Err(e) => {
            // Manually print the error so we can control the format.
            eprintln!("Exiting with error: {e}");
            std::process::exit(1);
        }
    }
}

async fn main_inner() -> Result<(), BoxError> {
    tracing::register_logger();

    let args = Args::parse();
    let Args {
        config: config_path,
        start,
        end_block,
        n_jobs,
        data_dir,
        partition_size_mb,
        disable_compression,
        dataset: datasets,
        run_every_mins,
    } = args;

    let config = Arc::new(Config::load(config_path, data_dir)?);
    let dataset_store = DatasetStore::new(config.clone());
    let partition_size = partition_size_mb * 1024 * 1024;
    let compression = if disable_compression {
        parquet::basic::Compression::UNCOMPRESSED
    } else {
        Compression::ZSTD(ZstdLevel::try_new(1).unwrap())
    };
    let parquet_opts = parquet_opts(compression, true);
    let end_block = end_block.map(|e| resolve_end_block(start, e)).transpose()?;
    let env = Arc::new(config.make_runtime_env()?);
    let run_every = run_every_mins.map(|s| tokio::time::interval(Duration::from_secs(s * 60)));

    match run_every {
        None => {
            for dataset_name in datasets {
                dump_dataset(
                    &dataset_name,
                    &dataset_store,
                    &config,
                    &env,
                    n_jobs,
                    partition_size,
                    &parquet_opts,
                    start,
                    end_block,
                )
                .await?
            }
        }
        Some(mut run_every) => loop {
            run_every.tick().await;

            for dataset_name in &datasets {
                dump_dataset(
                    dataset_name,
                    &dataset_store,
                    &config,
                    &env,
                    n_jobs,
                    partition_size,
                    &parquet_opts,
                    start,
                    end_block,
                )
                .await?;
            }
        },
    }

    Ok(())
}

// if end_block starts with "+" then it is a relative block number
// otherwise, it's an absolute block number and should be after start_block
fn resolve_end_block(start_block: u64, end_block: String) -> Result<u64, BoxError> {
    let end_block = if end_block.starts_with('+') {
        let relative_block = end_block
            .trim_start_matches('+')
            .parse::<u64>()
            .map_err(|e| format!("invalid relative end block: {e}"))?;
        start_block + relative_block
    } else {
        end_block
            .parse::<u64>()
            .map_err(|e| format!("invalid end block: {e}"))?
    };
    if end_block < start_block {
        return Err("end_block must be greater than or equal to start_block".into());
    }
    if end_block == 0 {
        return Err("end_block must be greater than 0".into());
    }
    Ok(end_block)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_block_range() {
        let test_cases = vec![
            (10, "20", Ok(20)),
            (0, "1", Ok(1)),
            (
                18446744073709551614,
                "18446744073709551615",
                Ok(18_446_744_073_709_551_615u64),
            ),
            (10, "+5", Ok(15)),
            (100, "90", Err(BoxError::from(""))),
            (0, "0", Err(BoxError::from(""))),
            (0, "0x", Err(BoxError::from(""))),
            (0, "xxx", Err(BoxError::from(""))),
            (100, "+1000x", Err(BoxError::from(""))),
            (100, "+1x", Err(BoxError::from(""))),
        ];

        for (start_block, end_block, expected) in test_cases {
            match resolve_end_block(start_block, end_block.into()) {
                Ok(result) => assert_eq!(expected.unwrap(), result),
                Err(_) => assert_eq!(expected.is_err(), true),
            }
        }
    }
}
