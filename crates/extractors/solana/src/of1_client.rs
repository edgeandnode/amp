use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use common::BoxResult;
use futures::{Stream, StreamExt};
use solana_clock::Slot;
use tokio::io::AsyncWriteExt;
pub use yellowstone_faithful_car_parser as of1_car_parser;

use crate::{metrics, rpc_client};

const OLD_FAITHFUL_ARCHIVE_URL: &str = "https://files.old-faithful.net";

#[derive(Debug, Default)]
pub(crate) struct DecodedBlock {
    pub(crate) slot: Slot,
    pub(crate) parent_slot: Slot,

    pub(crate) blockhash: [u8; 32],
    pub(crate) prev_blockhash: [u8; 32],

    pub(crate) block_height: Option<u64>,
    pub(crate) blocktime: u64,

    pub(crate) transactions: Vec<solana_sdk::transaction::VersionedTransaction>,
    pub(crate) transaction_metas: Vec<solana_storage_proto::confirmed_block::TransactionStatusMeta>,

    #[allow(dead_code)]
    pub(crate) block_rewards: Vec<solana_storage_proto::confirmed_block::Rewards>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn stream(
    start: solana_clock::Slot,
    end: solana_clock::Slot,
    of1_car_directory: PathBuf,
    solana_rpc_client: Arc<rpc_client::SolanaRpcClient>,
    get_block_config: rpc_client::rpc_config::RpcBlockConfig,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
    provider: String,
    network: String,
) -> impl Stream<Item = BoxResult<DecodedBlock>> {
    async_stream::stream! {
        let mut epoch = start / solana_clock::DEFAULT_SLOTS_PER_EPOCH;

        // Pre-fetch the initial previous block hash via JSON-RPC so that we don't have to
        // (potentially) read multiple Old Faithful CAR files to find it.
        let mut prev_blockhash = if start == 0 {
            [0u8; 32]
        } else {
            let mut parent_slot = start - 1;
            loop {
                let resp = solana_rpc_client
                    .get_block(parent_slot, get_block_config, metrics.clone())
                    .await;

                match resp {
                    // Found the parent block, extract its blockhash.
                    Ok(block) => {
                        break bs58::decode(block.blockhash)
                            .into_vec()?
                            .try_into()
                            .expect("blockhash is 32 bytes");
                    }
                    // Parent block is missing, try the previous slot.
                    Err(e) if rpc_client::is_block_missing_err(&e) => {
                        if parent_slot == 0 {
                            break [0u8; 32];
                        } else {
                            parent_slot -= 1;
                            continue;
                        }
                    }
                    // Some other error occurred.
                    Err(e) => {
                        yield Err(e.into());
                        return;
                    }
                }
            }
        };

        let reqwest_client = reqwest::Client::new();

        // Download historical data via Old Faithful archive CAR files.
        'epochs: loop {
            tracing::debug!(epoch, "processing Old Faithful CAR file");
            let local_filename = format!("epoch-{}.car", epoch);
            let epoch_car_file_path = of1_car_directory.join(local_filename);
            if let Err(e) = download_of1_car_file(
                epoch,
                &reqwest_client,
                &epoch_car_file_path,
                metrics.clone(),
                &provider,
                &network,
            ).await {
                if let FileDownloadError::Http(404) = e {
                    // No more epoch CAR files available.
                    break 'epochs;
                } else {
                    tracing::debug!("failed to download Old Faithful CAR file");

                    if let Some(metrics) = &metrics {
                        metrics.record_of1_car_download_error(epoch, &provider, &network);
                    }

                    yield Err(e.into());
                    return;
                }
            };

            let buf_reader = tokio::fs::File::open(&epoch_car_file_path).await.map(tokio::io::BufReader::new)?;
            let mut node_reader = of1_car_parser::node::NodeReader::new(buf_reader);

            while let Some(block) = read_entire_block(&mut node_reader, prev_blockhash).await? {
                prev_blockhash = block.blockhash;

                if block.slot < start {
                    // Skip blocks before the start of the requested range.
                    continue;
                }

                if block.slot > end {
                    // Reached the end of the requested range.
                    return;
                }

                yield Ok(block);
            }

            tracing::debug!(%epoch, "deleting processed Old Faithful CAR file");
            if let Err(e) = tokio::fs::remove_file(epoch_car_file_path).await
                && e.kind() != std::io::ErrorKind::NotFound {
                    yield Err(e.into());
                    return;
            }

            epoch += 1;
        }
    }
}

/// Downloads the Old Faithful CAR file for the given epoch into the specified output directory.
///
/// If the file was partially downloaded before, the download will resume from where it left off.
async fn download_of1_car_file(
    epoch: solana_clock::Epoch,
    reqwest_client: &reqwest::Client,
    dest: &Path,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
    provider: &str,
    network: &str,
) -> Result<(), FileDownloadError> {
    enum DownloadAction {
        Download,
        Resume(u64),
        Restart,
        Skip,
    }

    let download_url = of1_car_download_url(epoch);

    let action = match std::fs::metadata(dest).map(|meta| meta.len()) {
        Ok(0) => DownloadAction::Download,
        Ok(local_file_size) => {
            // Get the actual file size from the server to determine if we need to resume.
            let head_response = reqwest_client.head(&download_url).send().await?;

            if head_response.status() != reqwest::StatusCode::OK {
                return Err(FileDownloadError::Http(head_response.status().as_u16()));
            }

            let Some(content_length) = head_response.headers().get(reqwest::header::CONTENT_LENGTH)
            else {
                return Err(FileDownloadError::MissingContentLengthHeader);
            };
            let remote_file_size = content_length
                .to_str()
                .map_err(|_| FileDownloadError::ContentLengthParsing)?
                .parse()
                .map_err(|_| FileDownloadError::ContentLengthParsing)?;

            match local_file_size.cmp(&remote_file_size) {
                // Local file is partially downloaded, need to resume.
                std::cmp::Ordering::Less => DownloadAction::Resume(local_file_size),
                // Local file is larger than remote file, need to restart download.
                std::cmp::Ordering::Greater => DownloadAction::Restart,
                // File already fully downloaded.
                std::cmp::Ordering::Equal => DownloadAction::Skip,
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => DownloadAction::Download,
        Err(e) => return Err(FileDownloadError::Io(e)),
    };

    // Set up HTTP headers for range requests if the file already exists.
    let mut headers = reqwest::header::HeaderMap::new();

    match action {
        DownloadAction::Download => {
            tracing::debug!(%download_url, "downloading Old Faithful CAR file");
        }
        DownloadAction::Resume(download_offset) => {
            tracing::debug!(
                %download_url,
                %download_offset,
                "resuming Old Faithful CAR file download"
            );
            let range_header = format!("bytes={download_offset}-");
            let range_header_value =
                reqwest::header::HeaderValue::from_str(&range_header).expect("valid range header");
            headers.insert(reqwest::header::RANGE, range_header_value);
        }
        DownloadAction::Restart => {
            tracing::debug!(
                %download_url,
                "local Old Faithful CAR file is larger than remote file, restarting download"
            );
            tokio::fs::remove_file(&dest).await?;
        }
        DownloadAction::Skip => {
            tracing::debug!(
                %download_url,
                "local Old Faithful CAR file already fully downloaded, skipping"
            );
            return Ok(());
        }
    }

    let start = std::time::Instant::now();

    let response = reqwest_client
        .get(download_url)
        .headers(headers)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        return Err(FileDownloadError::Http(status.as_u16()));
    }

    if let DownloadAction::Resume(_) = action {
        // Expecting a 206 Partial Content response when resuming.
        if status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(FileDownloadError::PartialDownloadNotSupported);
        }
    }

    let mut file = tokio::fs::File::options()
        .create(true) // Create the file if it doesn't exist.
        .append(true) // Append to the file to support resuming.
        .open(&dest)
        .await?;

    // Stream the file content since these files can be extremely large.
    let mut stream = response.bytes_stream();
    let mut bytes_downloaded = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;

        file.write_all(&chunk).await?;

        bytes_downloaded += chunk.len() as u64;

        if let Some(ref metrics) = metrics {
            metrics.record_of1_car_download_bytes(chunk.len() as u64, epoch, provider, network);
        }
    }

    let duration = start.elapsed().as_secs_f64();

    tracing::debug!(
        %epoch,
        %bytes_downloaded,
        duration_secs = %duration,
        "downloaded Old Faithful CAR file"
    );

    if let Some(ref metrics) = metrics {
        metrics.record_of1_car_download(duration, epoch, provider, network);
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum FileDownloadError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error with status code: {0}")]
    Http(u16),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("missing Content-Length header in HTTP response")]
    MissingContentLengthHeader,
    #[error("error parsing Content-Length header")]
    ContentLengthParsing,
    #[error("partial downloads are not supported by the server")]
    PartialDownloadNotSupported,
}

/// Read an entire block worth of nodes from the given node reader and decode them into
/// a [DecodedBlock].
///
/// Inspired by the Old Faithful CAR parser example:
/// <https://github.com/lamports-dev/yellowstone-faithful-car-parser/blob/master/src/bin/counter.rs>
async fn read_entire_block<R: tokio::io::AsyncRead + Unpin>(
    node_reader: &mut of1_car_parser::node::NodeReader<R>,
    prev_blockhash: [u8; 32],
) -> BoxResult<Option<DecodedBlock>> {
    // TODO: Could this be executed while downloading the block? As in, read from a stream and
    // attempt to decode until successful.

    // Once we reach `Node::Block`, the node map will contain all of the nodes needed to reassemble
    // that block.
    let mut nodes = of1_car_parser::node::Nodes::read_until_block(node_reader).await?;

    let block = match nodes.nodes.pop() {
        Some((_, of1_car_parser::node::Node::Block(block))) => block,
        None | Some((_, of1_car_parser::node::Node::Epoch(_))) => return Ok(None),
        Some((cid, node)) => {
            let err = format!(
                "unexpected node while reading block: kind={:?}, cid={}",
                node.kind(),
                cid
            );
            return Err(err.into());
        }
    };

    let mut transactions = Vec::new();
    let mut transactions_meta = Vec::new();

    for entry_cid in &block.entries {
        let Some(of1_car_parser::node::Node::Entry(entry)) = nodes.nodes.get(entry_cid) else {
            let err = format!("expected entry node for cid {entry_cid}");
            return Err(err.into());
        };
        for tx_cid in &entry.transactions {
            let Some(of1_car_parser::node::Node::Transaction(tx)) = nodes.nodes.get(tx_cid) else {
                let err = format!("expected transaction node for cid {tx_cid}");
                return Err(err.into());
            };

            let tx_df = nodes.reassemble_dataframes(&tx.data)?;
            let tx_meta_df = nodes.reassemble_dataframes(&tx.metadata)?;

            let (tx, _) = bincode::serde::decode_from_slice(&tx_df, bincode::config::standard())?;
            let tx_meta = prost::Message::decode(&tx_meta_df[..])?;

            transactions.push(tx);
            transactions_meta.push(tx_meta);
        }
    }

    let last_entry_cid = block.entries.last().expect("at least one entry");
    let last_entry_node = nodes.nodes.get(last_entry_cid).expect("last entry node");
    let of1_car_parser::node::Node::Entry(last_entry) = last_entry_node else {
        let err = format!("expected entry node for cid {last_entry_cid}");
        return Err(err.into());
    };

    let blockhash: [u8; 32] = last_entry
        .hash
        .clone()
        .try_into()
        .expect("blockhash is 32 bytes");

    let block = DecodedBlock {
        slot: block.slot,
        parent_slot: block.meta.parent_slot,
        blockhash,
        prev_blockhash,
        block_height: block.meta.block_height,
        blocktime: block.meta.blocktime,
        transactions,
        transaction_metas: transactions_meta,
        // TODO: Work with rewards?
        #[allow(dead_code)]
        block_rewards: Vec::new(),
    };

    Ok(Some(block))
}

/// Generates the Old Faithful CAR download URL for the given epoch.
///
/// Reference: <https://docs.old-faithful.net/references/of1-files>.
fn of1_car_download_url(epoch: solana_clock::Epoch) -> String {
    format!("{OLD_FAITHFUL_ARCHIVE_URL}/{epoch}/epoch-{epoch}.car")
}
