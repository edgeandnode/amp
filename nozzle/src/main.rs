mod dev;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use alloy::transports::http::reqwest::Url;
use arrow_flight::flight_service_server::FlightServiceServer;
use axum::response::Response;
use axum::serve::ListenerExt;
use clap::Parser as _;
use common::manifest;
use common::{config::Config, tracing, BoxError};
use datafusion::catalog_common::resolve_table_references;
use datafusion::parquet;
use dataset_store::{sql_datasets, DatasetStore};
use futures::{StreamExt as _, TryStreamExt as _};
use log::info;
use metadata_db::MetadataDb;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use server::service::Service;
use tokio::net::TcpListener;
use tonic::transport::Server;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, clap::Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    Dev {
        #[arg(long, default_value = "./nozzle/")]
        nozzle_dir: PathBuf,
        #[arg(long, default_value = "http://localhost:8545")]
        rpc_url: Url,
    },
    Dump {
        #[arg(long, env = "NOZZLE_CONFIG")]
        config: String,

        /// The name of the dataset to dump. This will be looked up in the dataset definiton directory.
        /// Will also be used as a subdirectory in the output path, `<data_dir>/<dataset>`.
        ///
        /// Also accepts a comma-separated list of datasets, which will be dumped in the provided order.
        #[arg(long, required = true, env = "DUMP_DATASET", value_delimiter = ',')]
        dataset: Vec<String>,

        /// If set to true, only the listed datasets will be dumped in the order they are listed.
        /// By default dump listed datasets and their dependencies, ordered such that each dataset
        /// will be dumped after all datasets they depend on.
        #[arg(long, env = "DUMP_IGNORE_DEPS")]
        ignore_deps: bool,

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
    },
    Server {
        #[arg(long, env = "NOZZLE_CONFIG")]
        config: String,
    },
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
    match args.command {
        Command::Dev {
            nozzle_dir,
            rpc_url,
        } => {
            let addr: SocketAddr = ([0, 0, 0, 0], 1610).into();

            let nozzle = Arc::new(tokio::sync::Mutex::new(dev::Nozzle::new(nozzle_dir)?));
            nozzle.lock().await.add_rpc_dataset(rpc_url.as_str())?;

            let router = axum::Router::new()
                .route(
                    "/sql",
                    axum::routing::post(
                        |axum::extract::State(nozzle): axum::extract::State<
                            Arc<tokio::sync::Mutex<dev::Nozzle>>,
                        >,
                         request| async move {
                            let service = match nozzle.lock().await.service() {
                                Ok(service) => service,
                                Err(err) => return Response::new(json_error(err).into()),
                            };
                            handle_jsonl_request(&service, request).await
                        },
                    )
                    .with_state(nozzle.clone()),
                )
                .layer(
                    tower_http::compression::CompressionLayer::new()
                        .br(true)
                        .gzip(true),
                );
            let listener = TcpListener::bind(addr)
                .await?
                .tap_io(|tcp_stream| tcp_stream.set_nodelay(true).unwrap());
            axum::serve(listener, router).await?;
            Ok(())
        }
        Command::Dump {
            config,
            start,
            end_block,
            n_jobs,
            partition_size_mb,
            disable_compression,
            dataset: mut datasets,
            ignore_deps,
            run_every_mins,
        } => {
            let config = Arc::new(
                Config::load(config, true, None)
                    .map_err(|e| format!("failed to load config: {e}"))?,
            );
            let metadata_db = match &config.metadata_db_url {
                Some(url) => Some(MetadataDb::connect(url).await?),
                None => None,
            };

            let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
            let partition_size = partition_size_mb * 1024 * 1024;
            let compression = if disable_compression {
                parquet::basic::Compression::UNCOMPRESSED
            } else {
                Compression::ZSTD(ZstdLevel::try_new(1).unwrap())
            };
            let parquet_opts = dump::parquet_opts(compression, true);
            let end_block = end_block.map(|e| resolve_end_block(start, e)).transpose()?;
            let run_every =
                run_every_mins.map(|s| tokio::time::interval(Duration::from_secs(s * 60)));

            if !ignore_deps {
                datasets = datasets_and_dependencies(&dataset_store, datasets).await?;
            }

            let dump_order: Vec<&str> = datasets.iter().map(|d| d.as_str()).collect();
            info!("dump order: {}", dump_order.join(", "));

            match run_every {
                None => {
                    for dataset_name in datasets {
                        dump::dump_dataset(
                            &dataset_name,
                            &dataset_store,
                            &config,
                            metadata_db.as_ref(),
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
                        dump::dump_dataset(
                            dataset_name,
                            &dataset_store,
                            &config,
                            metadata_db.as_ref(),
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
        Command::Server { config } => {
            let config = Arc::new(
                Config::load(config, true, None)
                    .map_err(|e| format!("failed to load config: {e}"))?,
            );
            let metadata_db = match &config.metadata_db_url {
                Some(url) => Some(MetadataDb::connect(url).await?),
                None => None,
            };

            info!("memory limit is {} MB", config.max_mem_mb);
            info!(
                "spill to disk allowed: {}",
                !config.spill_location.is_empty()
            );

            let service = Service::new(config, metadata_db)?;

            let flight_addr: SocketAddr = ([0, 0, 0, 0], 1602).into();
            let flight_server = Server::builder()
                .add_service(FlightServiceServer::new(service.clone()))
                .serve(flight_addr);
            info!("Serving Arrow Flight RPC at {}", flight_addr);

            let jsonl_addr: SocketAddr = ([0, 0, 0, 0], 1603).into();
            let jsonl_server = run_jsonl_server(service, jsonl_addr);
            info!("Serving JSON lines at {}", jsonl_addr);

            tokio::select! {
                result = flight_server => result?,
                result = jsonl_server => result?,
            };
            Err("server shutdown unexpectedly, it should run forever".into())
        }
    }
}

async fn run_jsonl_server(service: Service, addr: SocketAddr) -> Result<(), BoxError> {
    let router = axum::Router::new()
        .route(
            "/",
            axum::routing::post(|axum::extract::State(service), request| async move {
                handle_jsonl_request(&service, request).await
            })
            .with_state(service),
        )
        .layer(
            tower_http::compression::CompressionLayer::new()
                .br(true)
                .gzip(true),
        );
    let listener = TcpListener::bind(addr)
        .await?
        .tap_io(|tcp_stream| tcp_stream.set_nodelay(true).unwrap());
    axum::serve(listener, router).await?;
    Ok(())
}

async fn handle_jsonl_request(service: &Service, request: String) -> axum::response::Response {
    let stream = match service.execute_query(&request).await {
        Ok(stream) => stream,
        Err(err) => return axum::response::Response::new(json_error(err.message()).into()),
    };
    let stream = stream
        .map(|result| -> Result<String, BoxError> {
            let batch = result.map_err(json_error)?;
            let mut buf: Vec<u8> = Default::default();
            let mut writer = arrow_json::writer::LineDelimitedWriter::new(&mut buf);
            writer.write(&batch)?;
            Ok(String::from_utf8(buf).unwrap())
        })
        .map_err(json_error);
    axum::response::Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
        .unwrap()
}

fn json_error(message: impl std::fmt::Display) -> String {
    format!(r#"{{"error": "{}"}}"#, message)
}

/// Return the input datasets and their dataset dependencies. The output set is ordered such that
/// each dataset comes after all datasets it depends on.
async fn datasets_and_dependencies(
    store: &Arc<DatasetStore>,
    mut datasets: Vec<String>,
) -> Result<Vec<String>, BoxError> {
    let mut deps: BTreeMap<String, Vec<String>> = Default::default();
    while !datasets.is_empty() {
        let dataset = store.load_dataset(&datasets.pop().unwrap()).await?;
        match dataset.kind.as_str() {
            sql_datasets::DATASET_KIND | manifest::DATASET_KIND => {
                deps.insert(dataset.name.clone(), vec![]);
            }
            _ => continue,
        };
        let sql_dataset = match dataset.kind.as_str() {
            sql_datasets::DATASET_KIND => store.load_sql_dataset(&dataset.name).await?,
            manifest::DATASET_KIND => store.load_manifest_dataset(&dataset.name).await?,
            _ => continue,
        };
        let mut refs: Vec<String> = Default::default();
        for query in sql_dataset.queries.values() {
            let (tables, _) = resolve_table_references(query, true)?;
            refs.append(
                &mut tables
                    .iter()
                    .filter_map(|t| t.schema())
                    .map(ToString::to_string)
                    .collect(),
            );
        }
        let mut untracked_refs = refs
            .iter()
            .filter(|r| deps.keys().all(|d| d != *r))
            .cloned()
            .collect();
        datasets.append(&mut untracked_refs);
        deps.insert(dataset.name, refs);
    }

    dependency_sort(deps)
}

/// Given a map of values to their dependencies, return a set where each value is ordered after
/// all of its dependencies. An error is returned if a cycle is detected.
fn dependency_sort(deps: BTreeMap<String, Vec<String>>) -> Result<Vec<String>, BoxError> {
    let nodes: BTreeSet<&String> = deps
        .iter()
        .flat_map(|(ds, deps)| std::iter::once(ds).chain(deps))
        .collect();
    let mut ordered: Vec<String> = Default::default();
    let mut visited: BTreeSet<&String> = Default::default();
    let mut visited_cycle: BTreeSet<&String> = Default::default();
    fn dfs<'a>(
        node: &'a String,
        deps: &'a BTreeMap<String, Vec<String>>,
        ordered: &mut Vec<String>,
        visited: &mut BTreeSet<&'a String>,
        visited_cycle: &mut BTreeSet<&'a String>,
    ) -> Result<(), BoxError> {
        if visited_cycle.contains(node) {
            return Err(format!("dependency cycle detected on dataset {node}").into());
        }
        if visited.contains(node) {
            return Ok(());
        }
        visited_cycle.insert(node);
        for dep in deps.get(node).into_iter().flatten() {
            dfs(dep, deps, ordered, visited, visited_cycle)?;
        }
        visited_cycle.remove(node);
        visited.insert(node);
        ordered.push(node.to_string());
        Ok(())
    }
    for node in nodes {
        if !visited.contains(node) {
            dfs(node, &deps, &mut ordered, &mut visited, &mut visited_cycle)?;
        }
    }
    Ok(ordered)
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
                Err(_) => assert!(expected.is_err()),
            }
        }
    }

    #[test]
    fn dependency_sort_order() {
        #[allow(clippy::type_complexity)]
        let cases: &[(&[(&str, &[&str])], Option<&[&str]>)] = &[
            (&[("a", &["b"]), ("b", &["a"])], None),
            (&[("a", &["b"])], Some(&["b", "a"])),
            (&[("a", &["b", "c"])], Some(&["b", "c", "a"])),
            (&[("a", &["b"]), ("c", &[])], Some(&["b", "a", "c"])),
            (&[("a", &["b"]), ("c", &["b"])], Some(&["b", "a", "c"])),
            (
                &[("a", &["b", "c"]), ("b", &["d"]), ("c", &["d"])],
                Some(&["d", "b", "c", "a"]),
            ),
            (
                &[("a", &["b", "c"]), ("b", &["c", "d"])],
                Some(&["c", "d", "b", "a"]),
            ),
        ];
        for (input, expected) in cases {
            let deps = input
                .iter()
                .map(|(k, v)| (k.to_string(), v.iter().map(ToString::to_string).collect()))
                .collect();
            let result = dependency_sort(deps);
            match expected {
                Some(expected) => assert_eq!(*expected, result.unwrap()),
                None => assert!(result.is_err()),
            }
        }
    }
}
