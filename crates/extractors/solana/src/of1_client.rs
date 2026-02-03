use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use backon::{ExponentialBuilder, Retryable};
use datasets_common::network_id::NetworkId;
use futures::{Stream, StreamExt};
use solana_clock::{Epoch, Slot};
use tokio::io::AsyncWriteExt;
pub use yellowstone_faithful_car_parser as car_parser;

use crate::{error::Of1StreamError, metrics, rpc_client};

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

pub(crate) enum CarManagerMessage {
    /// Request to download the CAR file for the given epoch. The oneshot sender will be
    /// notified when the download is complete. The boolean indicates whether the file
    /// was successfully downloaded (true) or if no file is available (false).
    DownloadFile((Epoch, tokio::sync::oneshot::Sender<bool>)),
    /// Notification that the CAR file for the given epoch has been processed and can
    /// be deleted if no other streams are interested in it.
    FileProcessed(Epoch),
}

pub(crate) type DecodedTransactionStatusMeta = DecodedField<
    solana_storage_proto::confirmed_block::TransactionStatusMeta,
    solana_storage_proto::StoredTransactionStatusMeta,
>;

pub(crate) type DecodedBlockRewards = DecodedField<
    solana_storage_proto::confirmed_block::Rewards,
    solana_storage_proto::StoredExtendedRewards,
>;

pub(crate) enum DecodedField<P, B> {
    Proto(P),
    Bincode(B),
}

pub(crate) struct DecodedSlot {
    pub(crate) slot: Slot,
    pub(crate) parent_slot: Slot,
    pub(crate) blockhash: [u8; 32],
    pub(crate) prev_blockhash: [u8; 32],
    pub(crate) block_height: Option<u64>,
    pub(crate) blocktime: i64,
    pub(crate) transactions: Vec<solana_sdk::transaction::VersionedTransaction>,
    pub(crate) transaction_metas: Vec<Option<DecodedTransactionStatusMeta>>,
    pub(crate) block_rewards: Option<DecodedBlockRewards>,
}

pub(crate) async fn car_file_manager(
    mut car_manager_rx: tokio::sync::mpsc::Receiver<CarManagerMessage>,
    car_directory: PathBuf,
    keep_car_files: bool,
    provider: String,
    network: NetworkId,
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
) -> impl Stream<Item = Result<DecodedSlot, Of1StreamError>> {
    async_stream::stream! {
        let mut epoch = start / solana_clock::DEFAULT_SLOTS_PER_EPOCH;

        // Pre-fetch the initial previous block hash via JSON-RPC so that we don't have to
        // (potentially) read multiple CAR files to find it.
        let mut prev_blockhash = if start == 0 {
            // Known previous blockhash for genesis mainnet block.
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
                            yield Err(Of1StreamError::PrevBlockhashNotFound(start));
                            return;
                        } else {
                            slot -= 1;
                        }
                }
                    Err(e) => {
                        yield Err(Of1StreamError::RpcClient(e));
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
                    yield Err(Of1StreamError::ChannelClosed(e));
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
                    yield Err(Of1StreamError::FileOpen(e));
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
                    yield Err(Of1StreamError::Mmap(e));
                    return;
                }
            };

            let mut node_reader = car_parser::node::NodeReader::new(&mmap[..]);

            while let Some(slot) = read_next_slot(&mut node_reader, prev_blockhash).await.transpose() {
                let slot = match slot {
                    Ok(slot) => slot,
                    Err(e) => {
                        car_manager_tx
                            .send(CarManagerMessage::FileProcessed(epoch))
                            .await
                            .expect("receiver not dropped");
                        yield Err(e);
                        return;
                    }
                };
                prev_blockhash = slot.blockhash;

                if slot.slot < start {
                    // Skip blocks before the start of the requested range.
                    continue;
                }

                match slot.slot.cmp(&end) {
                    std::cmp::Ordering::Less => {
                        yield Ok(slot);
                    },
                    std::cmp::Ordering::Equal => {
                        car_manager_tx
                            .send(CarManagerMessage::FileProcessed(epoch))
                            .await
                            .expect("receiver not dropped");
                        yield Ok(slot);
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
    network: &NetworkId,
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
async fn read_next_slot<R: tokio::io::AsyncRead + Unpin>(
    node_reader: &mut car_parser::node::NodeReader<R>,
    prev_blockhash: [u8; 32],
) -> Result<Option<DecodedSlot>, Of1StreamError> {
    /// Attempt to decode a field read from a CAR file as either protobuf or bincode encoded.
    /// Fail if both decoding attempts fail.
    ///
    /// For some epochs transaction metadata / block rewards are stored as protobuf encoded,
    /// while for others they are stored as bincode encoded. This function handles both cases.
    fn decode_proto_or_bincode<P, B>(
        field_name: &'static str,
        data: &[u8],
    ) -> Result<DecodedField<P, B>, Of1StreamError>
    where
        P: prost::Message + Default,
        B: serde::de::DeserializeOwned,
    {
        // All fields that need to be decoded this way are ZSTD compressed in CAR files.
        let decompressed = &*zstd::decode_all(data).map_err(|e| Of1StreamError::Zstd {
            field_name,
            error: e.to_string(),
        })?;
        match prost::Message::decode(decompressed).map(DecodedField::Proto) {
            Ok(data_proto) => Ok(data_proto),
            Err(prost_err) => match bincode::deserialize(decompressed).map(DecodedField::Bincode) {
                Ok(data_bincode) => Ok(data_bincode),
                Err(bincode_err) => {
                    let err = Of1StreamError::DecodeField {
                        field_name,
                        prost_err: prost_err.to_string(),
                        bincode_err: bincode_err.to_string(),
                    };
                    Err(err)
                }
            },
        }
    }

    // Once we reach `Node::Block`, the node map will contain all of the nodes needed to reassemble
    // that block.
    let nodes = car_parser::node::Nodes::read_until_block(node_reader)
        .await
        .map_err(Of1StreamError::NodeParse)?;

    let block = match nodes.nodes.last() {
        // Expected block node.
        Some((_, car_parser::node::Node::Block(block))) => block,
        // Reached end of CAR file.
        None | Some((_, car_parser::node::Node::Epoch(_))) => return Ok(None),
        Some((cid, node)) => {
            return Err(Of1StreamError::UnexpectedNode {
                kind: node.kind(),
                cid: (*cid).into(),
            });
        }
    };

    let mut transactions = Vec::new();
    let mut transaction_metas = Vec::new();

    for entry_cid in &block.entries {
        let Some(car_parser::node::Node::Entry(entry)) = nodes.nodes.get(entry_cid) else {
            return Err(Of1StreamError::MissingNode {
                expected: "entry",
                cid: entry_cid.to_string(),
            });
        };
        for tx_cid in &entry.transactions {
            let Some(car_parser::node::Node::Transaction(tx)) = nodes.nodes.get(tx_cid) else {
                return Err(Of1StreamError::MissingNode {
                    expected: "transaction",
                    cid: tx_cid.to_string(),
                });
            };

            let tx_df = nodes
                .reassemble_dataframes(&tx.data)
                .map_err(Of1StreamError::DataframeReassembly)?;
            let tx_meta_df = nodes
                .reassemble_dataframes(&tx.metadata)
                .map_err(Of1StreamError::DataframeReassembly)?;

            let tx = bincode::deserialize(&tx_df).map_err(Of1StreamError::Bincode)?;
            transactions.push(tx);

            let tx_meta = if tx_meta_df.is_empty() {
                None
            } else {
                decode_proto_or_bincode("tx_status_meta", tx_meta_df.as_slice()).map(Some)?
            };
            transaction_metas.push(tx_meta);
        }
    }

    let block_rewards = nodes
        .nodes
        .get(&block.rewards)
        .map(|rewards| {
            let car_parser::node::Node::Rewards(rewards) = rewards else {
                return Err(Of1StreamError::MissingNode {
                    expected: "block_rewards",
                    cid: block.rewards.to_string(),
                });
            };
            if rewards.slot != block.slot {
                return Err(Of1StreamError::RewardSlotMismatch {
                    expected: block.slot,
                    found: rewards.slot,
                });
            }

            nodes
                .reassemble_dataframes(&rewards.data)
                .map_err(Of1StreamError::DataframeReassembly)
                .and_then(|rewards_df| {
                    decode_proto_or_bincode("block_rewards", rewards_df.as_slice())
                })
        })
        .transpose()?;

    let blockhash = {
        // Hash of the last entry has the same value as that block's `blockhash` in
        // CAR files.
        let last_entry_cid = block.entries.last().expect("at least one entry");
        let last_entry_node = nodes.nodes.get(last_entry_cid).expect("last entry node");
        let car_parser::node::Node::Entry(last_entry) = last_entry_node else {
            return Err(Of1StreamError::MissingNode {
                expected: "entry",
                cid: last_entry_cid.to_string(),
            });
        };
        last_entry
            .hash
            .clone()
            .try_into()
            .expect("blockhash is 32 bytes")
    };

    let blocktime = block
        .meta
        .blocktime
        .try_into()
        .expect("blocktime fits in i64");

    let block = DecodedSlot {
        slot: block.slot,
        parent_slot: block.meta.parent_slot,
        blockhash,
        prev_blockhash,
        block_height: block.meta.block_height,
        blocktime,
        transactions,
        transaction_metas,
        block_rewards,
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
