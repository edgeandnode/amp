use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use backon::{ExponentialBuilder, Retryable};
use datasets_raw::BoxResult;
use futures::{Stream, StreamExt};
use solana_clock::{Epoch, Slot};
use tokio::io::AsyncWriteExt;
pub use yellowstone_faithful_car_parser as car_parser;

use crate::{metrics, rpc_client, tables};

const OLD_FAITHFUL_ARCHIVE_URL: &str = "https://files.old-faithful.net";

/// Maps epoch to a list of oneshot senders to notify when the download is complete.
type PendingMessageMap = HashMap<Epoch, Vec<tokio::sync::oneshot::Sender<bool>>>;

/// Maps epoch to the number of interested block streams. Each block stream will request
/// a file it wants to work with (CarManagerMessage::DownloadFile) and notify this task
/// when done (CarManagerMessage::FileProcessed). When the interest count reaches zero,
/// the file can be deleted.
///
/// NOTE: The interest data type should match max dump parallelism data type.
type FileInterestMap = HashMap<Epoch, u16>;

#[derive(Debug, Default)]
pub(crate) struct DecodedBlock {
    pub(crate) slot: Slot,
    pub(crate) parent_slot: Slot,

    pub(crate) blockhash: [u8; 32],
    pub(crate) prev_blockhash: [u8; 32],

    pub(crate) block_height: Option<u64>,
    pub(crate) blocktime: u64,

    pub(crate) transactions: Vec<solana_sdk::transaction::VersionedTransaction>,
    pub(crate) transaction_metas: Vec<tables::transactions::TransactionStatusMeta>,

    #[allow(dead_code)]
    pub(crate) block_rewards: Vec<solana_storage_proto::confirmed_block::Rewards>,
}

pub(crate) enum CarManagerMessage {
    /// Request to download the CAR file for the given epoch. The oneshot sender will be
    /// notified when the download is complete. The boolean indicates whether the file
    /// was successfully downloaded (true) or if no file is available (false).
    DownloadFile((Epoch, tokio::sync::oneshot::Sender<bool>)),
    /// Notification that the CAR file for the given epoch has been processed and can
    /// be deleted if no other streams are interested in it.
    FileProcessed(Epoch),
}

pub(crate) async fn car_file_manager(
    mut car_manager_rx: tokio::sync::mpsc::Receiver<CarManagerMessage>,
    car_directory: PathBuf,
    keep_car_files: bool,
    provider: String,
    network: String,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) {
    let mut downloaders = futures::stream::FuturesUnordered::new();
    let reqwest = Arc::new(reqwest::Client::new());
    let pending_msgs = Arc::new(Mutex::new(PendingMessageMap::new()));
    let file_interests = Arc::new(Mutex::new(FileInterestMap::new()));

    loop {
        tokio::select! {
            msg = car_manager_rx.recv() => {
                let Some(msg) = msg else {
                    tracing::debug!("CAR file manager channel closed, shutting down");
                    return;
                };
                match msg {
                    CarManagerMessage::DownloadFile((epoch, done_tx)) => {
                        tracing::debug!(%epoch, "received CAR file download message");
                        file_interests
                            .lock()
                            .unwrap()
                            .entry(epoch)
                            .and_modify(|count| *count += 1)
                            .or_insert(1);

                        {
                            let mut guard = pending_msgs.lock().unwrap();
                            let entry = guard.entry(epoch).or_default();
                            entry.push(done_tx);

                            if entry.len() > 1 {
                                // A download is already in progress for this epoch.
                                continue;
                            }
                        }

                        let reqwest = Arc::clone(&reqwest);
                        let pending_msgs = Arc::clone(&pending_msgs);
                        let car_directory = car_directory.clone();
                        let provider = provider.clone();
                        let network = network.clone();
                        let metrics = metrics.clone();

                        downloaders.push(tokio::spawn(async move {
                            let dest = car_directory.join(local_car_filename(epoch));

                            let result = (|| async {
                                ensure_car_file_exists(
                                    epoch,
                                    &reqwest,
                                    &dest,
                                    &provider,
                                    &network,
                                    metrics.clone()
                                ).await
                            })
                            .retry(ExponentialBuilder::default().without_max_times())
                            .sleep(tokio::time::sleep)
                            // Only retry on errors that are not HTTP 404, since that indicates
                            // that the CAR file for the gives epoch does not exist.
                            .when(|e| !matches!(e, FileDownloadError::Http(code) if *code == 404))
                            .notify(|error: &FileDownloadError, delay: Duration| {
                                if let Some(m) = metrics.as_ref() {
                                    m.record_of1_car_download_error(epoch, &provider, &network);
                                }
                                tracing::debug!(
                                    %epoch,
                                    %error,
                                    "CAR file download failed, retrying in {delay:?}"
                                );
                            }).await;

                            match result {
                                Ok(_) => {
                                    // CAR file is ready for use.
                                    tracing::debug!(%epoch, "CAR file is ready");
                                    let mut guard = pending_msgs.lock().unwrap();
                                    let pending = guard.remove(&epoch).expect("epoch previously inserted");
                                    for tx in pending {
                                        tx.send(true).ok();
                                    }
                                },
                                Err(FileDownloadError::Http(404)) => {
                                    // No more CAR files available.
                                    tracing::debug!(%epoch, "no CAR file available (404)");
                                    let mut guard = pending_msgs.lock().unwrap();
                                    let pending = guard.remove(&epoch).expect("epoch previously inserted");
                                    for tx in pending {
                                        tx.send(false).ok();
                                    }
                                },
                                _ => {
                                    unreachable!("all other errors should be retried indefinitely");
                                },
                            }
                        }));
                    }
                    CarManagerMessage::FileProcessed(epoch) => {
                        tracing::debug!(%epoch, "received CAR file processed message");
                        let should_delete = {
                            let mut guard = file_interests.lock().unwrap();
                            let count = guard.get_mut(&epoch).expect("epoch previously inserted");
                            *count -= 1;

                            if *count == 0 {
                                guard.remove(&epoch);
                                !keep_car_files
                            } else {
                                false
                            }
                        };

                        if should_delete {
                            // No more interested streams, delete the file.
                            let dest = car_directory.join(local_car_filename(epoch));
                            match tokio::fs::remove_file(&dest).await {
                                Ok(_) => {
                                    tracing::debug!(%epoch, "deleted processed CAR file");
                                }
                                // `ErrorKind::NotFound` is expected to occur for epochs that did
                                // not have a CAR file to begin with, since we still track interest
                                // in them.
                                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                                    tracing::debug!(%epoch, %error, "failed to delete CAR file");
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            // Drive the download tasks to completion.
            Some(_) = downloaders.next() => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn stream(
    start: solana_clock::Slot,
    end: solana_clock::Slot,
    car_directory: PathBuf,
    // The receiver part should never be dropped as the manager task ends only after all
    // streams are done.
    car_manager_tx: tokio::sync::mpsc::Sender<CarManagerMessage>,
    solana_rpc_client: Arc<rpc_client::SolanaRpcClient>,
    get_block_config: rpc_client::rpc_config::RpcBlockConfig,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> impl Stream<Item = BoxResult<DecodedBlock>> {
    async_stream::stream! {
        let mut epoch = start / solana_clock::DEFAULT_SLOTS_PER_EPOCH;

        // Pre-fetch the initial previous block hash via JSON-RPC so that we don't have to
        // (potentially) read multiple CAR files to find it.
        let mut prev_blockhash = if start == 0 {
            // Known previous blockhash for genesis block.
            bs58::decode("4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn")
                .into_vec()
                .map(TryInto::try_into)
                .expect("invalid base-58 string")
                .expect("blockhash is 32 bytes")
        } else {
            let mut slot = start;
            loop {
                let resp = solana_rpc_client
                    .get_block(slot, get_block_config, metrics.clone())
                    .await;

                match resp {
                    Ok(block) => {
                        break bs58::decode(block.previous_blockhash)
                            .into_vec()
                            .map(TryInto::try_into)
                            .expect("invalid base-58 string")
                            .expect("blockhash is 32 bytes");
                    },
                    Err(e) if rpc_client::is_block_missing_err(&e) => {
                        if slot == 0 {
                            let err = format!("could not find previous blockhash for slot {start}");
                            yield Err(err.into());
                            return;
                        } else {
                            slot -= 1;
                        }
                }
                    Err(e) => {
                        yield Err(e.into());
                        return;
                    }
                }
            }
        };

        // Download historical data via CAR files.
        loop {
            tracing::debug!(epoch, "processing historical epoch");
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let msg = CarManagerMessage::DownloadFile((epoch, done_tx));
            car_manager_tx.send(msg).await.expect("receiver not dropped");
            match done_rx.await {
                Ok(downloaded) if !downloaded => {
                    // No more CAR files available.
                    car_manager_tx
                        .send(CarManagerMessage::FileProcessed(epoch))
                        .await
                        .expect("receiver not dropped");
                    return;
                },
                Err(e) => {
                    car_manager_tx
                        .send(CarManagerMessage::FileProcessed(epoch))
                        .await
                        .expect("receiver not dropped");
                    yield Err(e.into());
                    return;
                }
                _ => {}
            }

            let dest = car_directory.join(local_car_filename(epoch));
            let file = match fs_err::File::open(dest) {
                Ok(f) => f,
                Err(e) => {
                    car_manager_tx
                        .send(CarManagerMessage::FileProcessed(epoch))
                        .await
                        .expect("receiver not dropped");
                    yield Err(e.into());
                    return;
                }
            };
            // SAFETY: The file is not modified/deleted while the mmap is in use.
            let mmap = match unsafe { memmap2::Mmap::map(&file) } {
                Ok(mmap) => mmap,
                Err(e) => {
                    car_manager_tx
                        .send(CarManagerMessage::FileProcessed(epoch))
                        .await
                        .expect("receiver not dropped");
                    yield Err(e.into());
                    return;
                }
            };

            let mut node_reader = car_parser::node::NodeReader::new(&mmap[..]);

            while let Some(block) = read_entire_block(&mut node_reader, prev_blockhash).await.transpose() {
                let block = match block {
                    Ok(block) => block,
                    Err(e) => {
                        car_manager_tx
                            .send(CarManagerMessage::FileProcessed(epoch))
                            .await
                            .expect("receiver not dropped");
                        yield Err(e);
                        return;
                    }
                };
                prev_blockhash = block.blockhash;

                if block.slot < start {
                    // Skip blocks before the start of the requested range.
                    continue;
                }

                match block.slot.cmp(&end) {
                    std::cmp::Ordering::Less => {
                        yield Ok(block);
                    },
                    std::cmp::Ordering::Equal => {
                        car_manager_tx
                            .send(CarManagerMessage::FileProcessed(epoch))
                            .await
                            .expect("receiver not dropped");
                        yield Ok(block);
                        return;
                    },
                    std::cmp::Ordering::Greater => {
                        car_manager_tx
                            .send(CarManagerMessage::FileProcessed(epoch))
                            .await
                            .expect("receiver not dropped");
                        return;
                    },
                }
            }

            car_manager_tx
                .send(CarManagerMessage::FileProcessed(epoch))
                .await
                .expect("receiver not dropped");

            epoch += 1;
        }
    }
}

/// Ensures that the entire CAR file for the given epoch exists at the specified destination path.
///
/// If the file was partially downloaded before, the download will resume from where it left off.
async fn ensure_car_file_exists(
    epoch: solana_clock::Epoch,
    reqwest: &reqwest::Client,
    dest: &Path,
    provider: &str,
    network: &str,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> Result<(), FileDownloadError> {
    enum DownloadAction {
        Download,
        Resume(u64),
        Restart,
        Skip,
    }

    let download_url = car_download_url(epoch);

    let action = match fs_err::metadata(dest).map(|meta| meta.len()) {
        Ok(0) => DownloadAction::Download,
        Ok(local_file_size) => {
            // Get the actual file size from the server to determine if we need to resume.
            let head_response = reqwest.head(&download_url).send().await?;

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
            tracing::debug!(%download_url, "downloading CAR file");
        }
        DownloadAction::Resume(download_offset) => {
            tracing::debug!(
                %download_url,
                %download_offset,
                "resuming CAR file download"
            );
            let range_header = format!("bytes={download_offset}-");
            let range_header_value =
                reqwest::header::HeaderValue::from_str(&range_header).expect("valid range header");
            headers.insert(reqwest::header::RANGE, range_header_value);
        }
        DownloadAction::Restart => {
            tracing::debug!(
                %download_url,
                "local CAR file is larger than remote file, restarting download"
            );
            tokio::fs::remove_file(&dest).await?;
        }
        DownloadAction::Skip => {
            tracing::debug!(
                %download_url,
                "local CAR file already fully downloaded, skipping download"
            );
            return Ok(());
        }
    }

    let start = std::time::Instant::now();

    let response = reqwest.get(download_url).headers(headers).send().await?;

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

        if let Some(m) = metrics.as_ref() {
            m.record_of1_car_download_bytes(chunk.len() as u64, epoch, provider, network);
        }
    }

    let duration = start.elapsed().as_secs_f64();
    tracing::debug!(%epoch, %bytes_downloaded, duration_secs = %duration, "downloaded CAR file");
    if let Some(m) = metrics.as_ref() {
        m.record_of1_car_download(duration, epoch, provider, network);
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
    node_reader: &mut car_parser::node::NodeReader<R>,
    prev_blockhash: [u8; 32],
) -> BoxResult<Option<DecodedBlock>> {
    // TODO: Could this be executed while downloading the block? As in, read from a stream and
    // attempt to decode until successful.

    // Once we reach `Node::Block`, the node map will contain all of the nodes needed to reassemble
    // that block.
    let mut nodes = car_parser::node::Nodes::read_until_block(node_reader).await?;

    let block = match nodes.nodes.pop() {
        Some((_, car_parser::node::Node::Block(block))) => block,
        None | Some((_, car_parser::node::Node::Epoch(_))) => return Ok(None),
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
        let Some(car_parser::node::Node::Entry(entry)) = nodes.nodes.get(entry_cid) else {
            let err = format!("expected entry node for cid {entry_cid}");
            return Err(err.into());
        };
        for (idx, tx_cid) in entry.transactions.iter().enumerate() {
            let Some(car_parser::node::Node::Transaction(tx)) = nodes.nodes.get(tx_cid) else {
                let err = format!("expected transaction node for cid {tx_cid}");
                return Err(err.into());
            };

            let tx_df = nodes.reassemble_dataframes(&tx.data)?;
            let tx_meta_df = nodes.reassemble_dataframes(&tx.metadata)?;

            let tx = bincode::deserialize(&tx_df)?;
            let tx_meta = if tx_meta_df.is_empty() {
                // Empty dataframe, return default transaction metadata.
                tables::transactions::TransactionStatusMeta::default()
            } else {
                let tx_index = idx.try_into().expect("conversion error");

                // Transaction metadata is ZSTD compressed in CAR files.
                let tx_meta = zstd::decode_all(tx_meta_df.as_slice()).expect("zstd decode error");
                let tx_meta = tx_meta.as_slice();

                // It seems that in CAR files some transaction metadata is protobuf
                // encoded and some is bincode encoded. We'll attempt both here and
                // only return an error if both of them fail.
                match prost::Message::decode(tx_meta) {
                    Ok(tx_meta_proto) => {
                        tables::transactions::TransactionStatusMeta::from_proto_meta(
                            block.slot,
                            tx_index,
                            tx_meta_proto,
                        )
                    }
                    Err(prost_err) => match bincode::deserialize(tx_meta) {
                        Ok(tx_meta_bincode) => {
                            tables::transactions::TransactionStatusMeta::from_stored_meta(
                                block.slot,
                                tx_index,
                                tx_meta_bincode,
                            )
                        }
                        Err(bincode_err) => {
                            let err = format!(
                                "failed to decode transaction metadata: prost_err={:?}, bincode_err={:?}",
                                prost_err, bincode_err
                            );
                            return Err(err.into());
                        }
                    },
                }
            };

            transactions.push(tx);
            transactions_meta.push(tx_meta);
        }
    }

    let blockhash = {
        // Hash of the last entry has the same value as that block's `blockhash` in
        // CAR files.
        let last_entry_cid = block.entries.last().expect("at least one entry");
        let last_entry_node = nodes.nodes.get(last_entry_cid).expect("last entry node");
        let car_parser::node::Node::Entry(last_entry) = last_entry_node else {
            let err = format!("expected entry node for cid {last_entry_cid}");
            return Err(err.into());
        };
        last_entry
            .hash
            .clone()
            .try_into()
            .expect("blockhash is 32 bytes")
    };

    let block = DecodedBlock {
        slot: block.slot,
        parent_slot: block.meta.parent_slot,
        blockhash,
        prev_blockhash: prev_blockhash.to_owned(),
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
fn car_download_url(epoch: solana_clock::Epoch) -> String {
    format!("{OLD_FAITHFUL_ARCHIVE_URL}/{epoch}/epoch-{epoch}.car")
}

fn local_car_filename(epoch: Epoch) -> String {
    format!("epoch-{epoch}.car")
}
