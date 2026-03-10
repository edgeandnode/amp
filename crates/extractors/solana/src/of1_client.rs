#[cfg(debug_assertions)]
use std::{collections::HashSet, sync::Mutex};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use amp_providers_common::{network_id::NetworkId, provider_name::ProviderName};
use futures::{FutureExt, Stream, StreamExt};
use yellowstone_faithful_car_parser as car_parser;

use crate::{error::Of1StreamError, metrics, rpc_client};

const BYTES_DOWNLOADED_METRICS_REPORT_THRESHOLD: u64 = 10 * 1024 * 1024; // 10 MiB

pub type DecodedTransactionStatusMeta = DecodedField<
    solana_storage_proto::confirmed_block::TransactionStatusMeta,
    solana_storage_proto::StoredTransactionStatusMeta,
>;

pub type DecodedBlockRewards = DecodedField<
    solana_storage_proto::confirmed_block::Rewards,
    solana_storage_proto::StoredExtendedRewards,
>;

pub enum DecodedField<P, B> {
    Proto(P),
    Bincode(B),
}

#[derive(Default)]
pub struct DecodedSlot {
    pub slot: solana_clock::Slot,
    pub parent_slot: solana_clock::Slot,
    pub blockhash: [u8; 32],
    pub prev_blockhash: [u8; 32],
    pub block_height: Option<u64>,
    pub blocktime: i64,
    pub transactions: Vec<solana_sdk::transaction::VersionedTransaction>,
    pub transaction_metas: Vec<Option<DecodedTransactionStatusMeta>>,
    pub block_rewards: Option<DecodedBlockRewards>,
}

impl DecodedSlot {
    /// Create a dummy `DecodedSlot` with the given slot number and default values for all
    /// other fields. This can be used for testing or as a placeholder when only the slot
    /// number is relevant.
    ///
    /// NOTE: The reason this is marked as `pub` is because it is used in integration tests
    /// in the `tests` crate.
    #[doc(hidden)]
    pub fn dummy(slot: solana_clock::Slot) -> Self {
        Self {
            slot,
            parent_slot: slot.saturating_sub(1),
            ..Default::default()
        }
    }
}

/// Context for OF1 streaming that can be passed to functions that need to report metrics.
#[derive(Debug, Clone)]
pub struct MetricsContext {
    pub provider: ProviderName,
    pub network: NetworkId,
    pub registry: Arc<metrics::MetricsRegistry>,
}

/// Create a stream of decoded slots for the given epoch by reading from the
/// corresponding CAR file downloaded from the Old Faithful archive.
#[allow(clippy::too_many_arguments)]
pub fn stream(
    start: solana_clock::Slot,
    end: solana_clock::Slot,
    reqwest: Arc<reqwest::Client>,
    solana_rpc_client: Arc<rpc_client::SolanaRpcClient>,
    get_block_config: rpc_client::rpc_config::RpcBlockConfig,
    metrics: Option<MetricsContext>,
    #[cfg(debug_assertions)] epochs_in_progress: Arc<Mutex<HashSet<solana_clock::Epoch>>>,
) -> impl Stream<Item = Result<DecodedSlot, Of1StreamError>> {
    async_stream::stream! {
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
                let metrics = metrics.clone().map(|m| m.registry);
                let resp = solana_rpc_client
                    .get_block(slot, get_block_config, metrics)
                    .await;

                match resp {
                    Ok(block) => {
                        break bs58::decode(block.previous_blockhash)
                            .into_vec()
                            .map(TryInto::try_into)
                            .expect("invalid base-58 string")
                            .expect("blockhash is 32 bytes");
                    }
                    Err(e) if rpc_client::is_block_missing_err(&e) => slot += 1,
                    Err(e) => {
                        yield Err(Of1StreamError::RpcClient(e));
                        return;
                    }
                }
            }
        };

        let start_epoch = start / solana_clock::DEFAULT_SLOTS_PER_EPOCH;
        let end_epoch = end / solana_clock::DEFAULT_SLOTS_PER_EPOCH;

        for epoch in start_epoch..=end_epoch {
            #[cfg(debug_assertions)]
            let _guard = epoch_supervision::Guard::new(epochs_in_progress.as_ref(), epoch);

            let reader = CarReader::new(
                epoch,
                reqwest.clone(),
                metrics.clone()
            );
            let mut node_reader = car_parser::node::NodeReader::new(reader);

            while let Some(slot) = read_next_slot(&mut node_reader, prev_blockhash)
                .await
                .transpose()
            {
                let slot = match slot {
                    Ok(slot) => slot,
                    // IO errors from the node reader could come from the underlying `CarReader`.
                    // Try to downcast to `CarReaderError` to determine how to map into `Of1StreamError`.
                    //
                    // NOTE: There should be no retry logic here because the `CarReader` should
                    // handle all retry logic internally and only return an error when a non-recoverable
                    // error occurs.
                    Err(Of1StreamError::NodeParse(car_parser::node::NodeError::Io(io_err)))
                        if io_err.kind() == std::io::ErrorKind::Other =>
                    {
                        let downcast = io_err.get_ref().and_then(|e| e.downcast_ref::<CarReaderError>());
                        let car_reader_err = match downcast {
                            None => {
                                CarReaderError::Io(io_err)
                            }
                            Some(CarReaderError::RangeRequestUnsupported) => {
                                CarReaderError::RangeRequestUnsupported
                            }
                            // No more CAR files available, end the stream.
                            Some(CarReaderError::FileNotFound) => {
                                return;
                            }
                            Some(e) => {
                                let error = e.to_string();
                                unreachable!(
                                    "unexpected CAR reader error (should be retried indefinetly); {error}"
                                );
                            }
                        };

                        yield Err(Of1StreamError::FileStream(car_reader_err));
                        return;
                    }
                    Err(e) => {
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
                    }
                    std::cmp::Ordering::Equal => {
                        yield Ok(slot);
                        return;
                    }
                    std::cmp::Ordering::Greater => {
                        return;
                    }
                }
            }
        }
    }
}

/// Errors that can occur during streaming of Solana blocks from Old Faithful
/// v1 (OF1) CAR files.
#[derive(Debug, thiserror::Error)]
pub enum CarReaderError {
    /// IO error when reading the CAR file.
    ///
    /// This can occur due to network issues, file corruption, or other problems when
    /// accessing the CAR file.
    #[error("IO error: {0}")]
    Io(#[source] std::io::Error),
    /// Reqwest error when connecting to or reading from the CAR file.
    ///
    /// This can occur due to network issues, timeouts, or other problems when making
    /// HTTP requests to access the CAR file.
    #[error("Reqwest error: {0}")]
    Reqwest(#[source] reqwest::Error),
    /// The CAR file for the requested epoch was not found (HTTP 404).
    ///
    /// This is a non-recoverable error because it indicates that the expected data
    /// is not available and retrying will not resolve the issue.
    #[error("CAR file not found (HTTP 404)")]
    FileNotFound,
    /// The server does not support HTTP range requests.
    ///
    /// This is a non-recoverable error because the [`CarReader`] relies on range
    /// requests to be able to resume interrupted downloads without re-downloading
    /// the entire CAR.
    #[error("server does not support range requests")]
    RangeRequestUnsupported,
}

/// Read an entire block worth of nodes from the given node reader and decode them into
/// a [DecodedSlot].
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
        let decompressed_data = zstd::decode_all(data).map_err(|e| Of1StreamError::Zstd {
            field_name,
            error: e.to_string(),
        })?;
        match prost::Message::decode(&*decompressed_data).map(DecodedField::Proto) {
            Ok(data_proto) => Ok(data_proto),
            Err(prost_err) => {
                match bincode::deserialize(&decompressed_data).map(DecodedField::Bincode) {
                    Ok(data_bincode) => Ok(data_bincode),
                    Err(bincode_err) => {
                        let err = Of1StreamError::DecodeField {
                            field_name,
                            decompressed_data,
                            prost_err: prost_err.to_string(),
                            bincode_err: bincode_err.to_string(),
                        };
                        Err(err)
                    }
                }
            }
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
                let meta = match decode_proto_or_bincode("tx_status_meta", tx_meta_df.as_slice()) {
                    Ok(meta) => meta,
                    // With transaction status meta we could be working with the legacy bincode format
                    // (in which some fields were not added yet) so we have to try to decode that as well.
                    Err(Of1StreamError::DecodeField {
                        field_name,
                        decompressed_data,
                        prost_err,
                        bincode_err,
                    }) => {
                        let legacy_meta = bincode::deserialize::<
                            solana_storage_proto::legacy::StoredTransactionStatusMeta,
                        >(&decompressed_data);

                        match legacy_meta {
                            // The legacy bincode format was successfully decoded, convert it to
                            // non-legacy format and return it.
                            Ok(meta) => DecodedTransactionStatusMeta::Bincode(meta.into()),
                            // Both decoding attempts failed, return the original error with additional
                            // legacy bincode error details.
                            Err(e) => {
                                let err = Of1StreamError::DecodeField {
                                    field_name,
                                    decompressed_data,
                                    prost_err,
                                    bincode_err: format!(
                                        "bincode error: {bincode_err}, legacy bincode error: {e}"
                                    ),
                                };
                                return Err(err);
                            }
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                };
                Some(meta)
            };
            transaction_metas.push(tx_meta);
        }
    }

    let block_rewards = nodes
        .nodes
        .get(&block.rewards)
        .map(|rewards| {
            let car_parser::node::Node::Rewards(rewards) = rewards else {
                return Err(Of1StreamError::UnexpectedNode {
                    kind: rewards.kind(),
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

type ConnectFuture = Pin<Box<dyn Future<Output = reqwest::Result<reqwest::Response>> + Send>>;
type ByteStream = Pin<Box<dyn Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>;
type BackoffFuture = Pin<Box<tokio::time::Sleep>>;

struct ByteStreamMonitor {
    epoch: solana_clock::Epoch,
    bytes_read_chunk: u64,
    started_at: std::time::Instant,
    provider: ProviderName,
    network: NetworkId,
    registry: Arc<metrics::MetricsRegistry>,
}

struct MonitoredByteStream {
    stream: ByteStream,
    monitor: Option<ByteStreamMonitor>,
}

impl MonitoredByteStream {
    fn new(
        stream: impl Stream<Item = reqwest::Result<bytes::Bytes>> + Send + 'static,
        epoch: solana_clock::Epoch,
        metrics: Option<MetricsContext>,
    ) -> Self {
        let stream = Box::pin(stream);
        let monitor = metrics.map(|metrics| ByteStreamMonitor {
            epoch,
            bytes_read_chunk: 0,
            started_at: std::time::Instant::now(),
            provider: metrics.provider,
            network: metrics.network,
            registry: metrics.registry,
        });
        Self { stream, monitor }
    }
}

impl Stream for MonitoredByteStream {
    type Item = <ByteStream as Stream>::Item;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = this.stream.poll_next_unpin(cx);

        if let Some(m) = this.monitor.as_mut() {
            match &poll {
                std::task::Poll::Ready(Some(Ok(bytes))) => {
                    m.bytes_read_chunk += bytes.len() as u64;

                    if m.bytes_read_chunk >= BYTES_DOWNLOADED_METRICS_REPORT_THRESHOLD {
                        m.registry.record_of1_car_download_bytes(
                            m.bytes_read_chunk,
                            m.epoch,
                            &m.provider,
                            &m.network,
                        );
                        m.bytes_read_chunk = 0;
                    }
                }
                std::task::Poll::Ready(Some(Err(_))) => {
                    m.registry
                        .record_of1_car_download_error(m.epoch, &m.provider, &m.network);
                }
                std::task::Poll::Ready(None) => {
                    // Record any remaining bytes read that didn't reach the reporting threshold.
                    if m.bytes_read_chunk > 0 {
                        m.registry.record_of1_car_download_bytes(
                            m.bytes_read_chunk,
                            m.epoch,
                            &m.provider,
                            &m.network,
                        );
                    }
                    let elapsed = m.started_at.elapsed().as_secs_f64();
                    m.registry
                        .record_of1_car_download(elapsed, m.epoch, &m.provider, &m.network);
                }
                _ => {}
            }
        }

        poll
    }
}

enum ReaderState {
    /// A single in-flight HTTP request to (re)connect.
    Connect(ConnectFuture),
    /// We have an active byte stream.
    Stream(MonitoredByteStream),
    /// We are waiting until a backoff deadline before attempting reconnect.
    Backoff(BackoffFuture),
}

struct CarReader {
    url: String,
    epoch: solana_clock::Epoch,
    reqwest: Arc<reqwest::Client>,
    state: ReaderState,
    overflow: Vec<u8>,
    bytes_read_total: u64,

    // Backoff control
    reconnect_attempt: u32,
    max_backoff: Duration,
    base_backoff: Duration,

    metrics: Option<MetricsContext>,
}

impl CarReader {
    fn new(
        epoch: solana_clock::Epoch,
        reqwest: Arc<reqwest::Client>,
        metrics: Option<MetricsContext>,
    ) -> Self {
        let url = car_download_url(epoch);
        let connect_fut = get_with_range_header(reqwest.clone(), url.clone(), 0);

        Self {
            url,
            epoch,
            reqwest,
            state: ReaderState::Connect(Box::pin(connect_fut)),
            overflow: Vec::new(),
            bytes_read_total: 0,
            reconnect_attempt: 0,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            metrics,
        }
    }

    fn schedule_backoff(&mut self, reason: impl std::fmt::Display) {
        self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
        let backoff = compute_backoff(self.base_backoff, self.max_backoff, self.reconnect_attempt);

        let backoff_str = format!("{:.1}s", backoff.as_secs_f32());
        tracing::warn!(
            epoch = self.epoch,
            bytes_read = self.bytes_read_total,
            attempt = self.reconnect_attempt,
            reason = %reason,
            backoff = %backoff_str,
            "CAR reader failed; scheduled retry"
        );

        let fut = tokio::time::sleep(backoff);
        self.state = ReaderState::Backoff(Box::pin(fut));
    }
}

impl tokio::io::AsyncRead for CarReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();

        // Drain overflow first.
        if !this.overflow.is_empty() {
            let to_copy = this.overflow.len().min(buf.remaining());
            buf.put_slice(&this.overflow[..to_copy]);
            this.overflow.drain(..to_copy);
            return std::task::Poll::Ready(Ok(()));
        }

        // Retry loop, return on successful read, EOF, or non-recoverable error (RangeRequestUnsupported).
        loop {
            match &mut this.state {
                ReaderState::Connect(fut) => match fut.as_mut().poll(cx) {
                    std::task::Poll::Ready(Ok(resp)) => {
                        let status = resp.status();
                        // Handle error codes.
                        match status {
                            reqwest::StatusCode::NOT_FOUND => {
                                let e = std::io::Error::other(CarReaderError::FileNotFound);
                                return std::task::Poll::Ready(Err(e));
                            }
                            status if !status.is_success() => {
                                this.schedule_backoff(format!("HTTP error: {status}"));
                                continue;
                            }
                            _ => {}
                        }

                        // Handle partial content.
                        if this.bytes_read_total > 0
                            && status != reqwest::StatusCode::PARTIAL_CONTENT
                        {
                            let e = std::io::Error::other(CarReaderError::RangeRequestUnsupported);
                            return std::task::Poll::Ready(Err(e));
                        }

                        // Initial connection succeeded, start reading the byte stream.
                        this.reconnect_attempt = 0;
                        let stream = MonitoredByteStream::new(
                            resp.bytes_stream(),
                            this.epoch,
                            this.metrics.clone(),
                        );
                        this.state = ReaderState::Stream(stream);
                    }
                    std::task::Poll::Ready(Err(e)) => {
                        this.schedule_backoff(format!("request error: {e}"));
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },
                ReaderState::Stream(stream) => match stream.poll_next_unpin(cx) {
                    // Reached EOF.
                    std::task::Poll::Ready(None) => {
                        return std::task::Poll::Ready(Ok(()));
                    }
                    // Read some bytes, account for possible overflow.
                    std::task::Poll::Ready(Some(Ok(bytes))) => {
                        let n_read = bytes.len();
                        let to_copy = n_read.min(buf.remaining());

                        buf.put_slice(&bytes[..to_copy]);
                        this.overflow.extend_from_slice(&bytes[to_copy..]);
                        this.bytes_read_total += n_read as u64;

                        return std::task::Poll::Ready(Ok(()));
                    }
                    std::task::Poll::Ready(Some(Err(e))) => {
                        this.schedule_backoff(format!("stream error: {e}"));
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },
                ReaderState::Backoff(fut) => match fut.poll_unpin(cx) {
                    std::task::Poll::Ready(()) => {
                        let fut = get_with_range_header(
                            this.reqwest.clone(),
                            this.url.clone(),
                            this.bytes_read_total,
                        );
                        this.state = ReaderState::Connect(Box::pin(fut));
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },
            }
        }
    }
}

async fn get_with_range_header(
    reqwest: Arc<reqwest::Client>,
    url: String,
    offset: u64,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut req = reqwest.get(&url);
    if offset > 0 {
        req = req.header(reqwest::header::RANGE, format!("bytes={offset}-"));
    }

    req.send().await
}

fn compute_backoff(base: Duration, cap: Duration, attempt: u32) -> Duration {
    // attempt=1 => base, attempt=2 => 2*base, attempt=3 => 4*base, ...
    let factor = 1u64 << attempt.saturating_sub(1).min(30);
    let backoff = base.saturating_mul(factor as u32);
    backoff.min(cap)
}

/// Generates the Old Faithful CAR download URL for the given epoch.
///
/// Reference: <https://docs.old-faithful.net/references/of1-files>.
fn car_download_url(epoch: solana_clock::Epoch) -> String {
    format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car")
}

#[cfg(debug_assertions)]
mod epoch_supervision {
    use super::{HashSet, Mutex};

    /// Guard that tracks in-progress epochs to detect overlapping Solana streams in debug builds.
    ///
    /// # Panics
    ///
    /// Panics if an attempt is made to [create](Guard::new) a guard for an epoch that is already
    /// in progress, or if a guard is dropped for an epoch that is not currently in progress.
    pub struct Guard<'a> {
        epoch: solana_clock::Epoch,
        in_progress_epochs: &'a Mutex<HashSet<solana_clock::Epoch>>,
    }

    impl<'a> Guard<'a> {
        pub fn new(
            in_progress_epochs: &'a Mutex<HashSet<solana_clock::Epoch>>,
            epoch: solana_clock::Epoch,
        ) -> Self {
            let mut epochs_in_progress = in_progress_epochs.lock().unwrap();
            let is_new = epochs_in_progress.insert(epoch);
            assert!(
                is_new,
                "epoch {epoch} already in progress, overlapping Solana streams are not allowed"
            );
            Self {
                epoch,
                in_progress_epochs,
            }
        }
    }

    impl<'a> Drop for Guard<'a> {
        fn drop(&mut self) {
            let mut epochs_in_progress = self.in_progress_epochs.lock().unwrap();
            let removed = epochs_in_progress.remove(&self.epoch);
            assert!(
                removed,
                "epoch {} was not in progress during drop, this should never happen",
                self.epoch
            );
        }
    }
}
