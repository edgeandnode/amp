use std::path::{Path, PathBuf};

use common::BoxResult;
use futures::{Stream, StreamExt};
use solana_clock::Slot;
use tokio::io::AsyncWriteExt;
pub use yellowstone_faithful_car_parser as of1_car_parser;

use crate::rpc_client;

const OLD_FAITHFUL_ARCHIVE_URL: &str = "https://files.old-faithful.net";

pub(crate) fn stream(
    start: solana_clock::Slot,
    end: solana_clock::Slot,
    of1_car_directory: PathBuf,
    solana_rpc_client: rpc_client::SolanaRpcClient,
    get_block_config: rpc_client::rpc_config::RpcBlockConfig,
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
                    .get_block(parent_slot, get_block_config)
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

        // Download historical data via Old Faithful archive CAR files.
        'epochs: loop {
            tracing::debug!(epoch, "processing Old Faithful epoch CAR file");
            let epoch_car_file_path = of1_car_directory.join(of1_car_filename(epoch));

            if !std::fs::exists(&epoch_car_file_path)? {
                // This can take a while.
                if let Err(e) = download_of1_car_file(epoch, &of1_car_directory).await {
                    if let FileDownloadError::Http(404) = e {
                        // No more epoch CAR files available.
                        break 'epochs;
                    } else {
                        yield Err(e.into());
                        return;
                    }
                };
            }

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

            epoch += 1;
        }
    }
}

/// Downloads the Old Faithful CAR file for the given epoch into the specified output directory.
async fn download_of1_car_file(
    epoch: solana_clock::Epoch,
    output_dir: &Path,
) -> Result<(), FileDownloadError> {
    let filename = of1_car_filename(epoch);
    let car_file_url = format!("{OLD_FAITHFUL_ARCHIVE_URL}/{epoch}/{filename}");
    tracing::info!(%car_file_url, "downloading Old Faithful CAR file");

    let car_file_path = output_dir.join(filename);
    let mut file = tokio::fs::File::create(&car_file_path).await?;

    let response = reqwest::get(&car_file_url).await?;
    let status = response.status();
    if !status.is_success() {
        tracing::debug!(
            %status,
            "failed to download Old Faithful CAR file"
        );
        return Err(FileDownloadError::Http(status.as_u16()));
    }

    // Stream the file content since these files can be extremely large.
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await.transpose()? {
        file.write_all(&chunk).await?;
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

            let tx = bincode::serde::decode_from_slice(&tx_df, bincode::config::standard())
                .map(|(tx, _)| tx)?;
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

#[derive(Debug, Default)]
pub(crate) struct DecodedBlock {
    pub(crate) slot: Slot,
    pub(crate) parent_slot: Slot,

    pub(crate) blockhash: [u8; 32],
    pub(crate) prev_blockhash: [u8; 32],

    pub(crate) block_height: Option<u64>,
    pub(crate) blocktime: u64,

    pub(crate) transactions: Vec<solana_sdk::transaction::VersionedTransaction>,
    pub(crate) transaction_metas:
        Vec<solana_storage_proto::convert::generated::TransactionStatusMeta>,

    #[allow(dead_code)]
    pub(crate) block_rewards: Vec<solana_storage_proto::convert::generated::Rewards>,
}

/// Generates the Old Faithful epoch CAR filename for the given epoch.
///
/// Reference: <https://docs.old-faithful.net/references/of1-files>.
fn of1_car_filename(epoch: solana_clock::Epoch) -> String {
    format!("epoch-{}.car", epoch)
}
