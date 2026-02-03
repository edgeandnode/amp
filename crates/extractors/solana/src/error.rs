use datasets_raw::rows::TableRowError;
pub use yellowstone_faithful_car_parser as car_parser;
use yellowstone_faithful_car_parser::node::{NodeError, ReassableError};

/// Errors that occur when converting Solana block data to table rows.
///
/// These errors indicate issues during the transformation of raw Solana blockchain
/// data (transactions, messages, instructions) into the tabular format used for storage.
#[derive(Debug, thiserror::Error)]
pub enum RowConversionError {
    /// Failed to build table rows from converted data.
    ///
    /// This occurs when the Arrow table builder fails to construct the final
    /// record batch, typically due to schema mismatches or memory allocation issues.
    #[error("failed to build table rows")]
    TableBuild(#[source] TableRowError),

    /// Encountered an unexpected transaction encoding format.
    ///
    /// Solana transactions can be encoded in different formats (binary, base58, base64).
    /// This error occurs when the transaction uses an encoding format that the extractor
    /// does not support or cannot parse.
    #[error("unexpected transaction encoding at slot {slot}, tx index {tx_idx}")]
    UnexpectedTransactionEncoding { slot: u64, tx_idx: usize },

    /// Encountered an unexpected message format in a transaction.
    ///
    /// Solana transaction messages can be in legacy or versioned formats. This error
    /// occurs when the message format does not match the expected structure, preventing
    /// extraction of account keys and instructions.
    #[error("unexpected message format at slot {slot}, tx index {tx_idx}")]
    UnexpectedMessageFormat { slot: u64, tx_idx: usize },

    /// Found a parsed inner instruction which is not supported.
    ///
    /// The extractor expects raw/binary inner instructions for processing. Parsed
    /// instructions (JSON-decoded by the RPC) cannot be converted back to the required
    /// binary format for consistent storage.
    #[error(
        "found parsed inner instruction at slot {slot}, tx index {tx_idx}, which is not supported"
    )]
    ParsedInnerInstructionNotSupported { slot: u64, tx_idx: usize },
}

/// Errors that occur during Old Faithful v1 (OF1) block streaming.
///
/// OF1 is a historical data source for Solana that provides blocks via CAR
/// (Content Addressable aRchive) files. These errors cover RPC communication,
/// file handling, and CAR parsing failures.
#[derive(Debug, thiserror::Error)]
pub enum Of1StreamError {
    /// Failed to communicate with the Solana RPC client.
    ///
    /// This occurs when querying the RPC for slot information or block data,
    /// typically due to network issues, rate limiting, or RPC node unavailability.
    #[error("RPC client error")]
    RpcClient(#[source] solana_client::client_error::ClientError),

    /// Could not find the previous blockhash for the given slot.
    ///
    /// When streaming blocks, the extractor needs the previous block's hash to
    /// maintain chain continuity. This error occurs when walking back through
    /// slots fails to find a valid parent block.
    #[error("could not find previous blockhash for slot {0}")]
    PrevBlockhashNotFound(u64),

    /// The CAR manager communication channel was closed unexpectedly.
    ///
    /// The CAR manager runs as a separate task handling file downloads. This error
    /// occurs when the channel for communicating with the manager is closed before
    /// a response is received, indicating the manager task has terminated.
    #[error("CAR manager channel closed")]
    ChannelClosed(#[source] tokio::sync::oneshot::error::RecvError),

    /// Failed to open a CAR file from disk.
    ///
    /// This occurs when the downloaded CAR file cannot be opened for reading,
    /// due to permission issues, file corruption, or the file being deleted
    /// between download and read.
    #[error("failed to open CAR file")]
    FileOpen(#[source] std::io::Error),

    /// Failed to memory-map a CAR file.
    ///
    /// CAR files are memory-mapped for efficient reading. This error occurs when
    /// the memory-mapping operation fails, typically due to insufficient virtual
    /// memory or file access issues.
    #[error("failed to memory-map CAR file")]
    Mmap(#[source] std::io::Error),

    /// Encountered an unexpected node type while reading a block from CAR.
    ///
    /// CAR files contain a DAG of nodes with specific expected types (blocks,
    /// transactions, etc.). This error occurs when a node's kind does not match
    /// what the parser expects at that position in the structure.
    #[error("unexpected node while reading block: kind={kind:?}, cid={cid}")]
    UnexpectedNode {
        kind: car_parser::node::Kind,
        cid: String,
    },

    /// Expected a specific node type but it was not found.
    ///
    /// When traversing the CAR DAG, certain CIDs are expected to resolve to
    /// specific node types. This error occurs when a referenced CID does not
    /// exist or resolves to a different type than expected.
    #[error("expected '{expected}' node for cid '{cid}'")]
    MissingNode { expected: &'static str, cid: String },

    /// Block reward node slot does not match the expected slot.
    ///
    /// When processing block reward nodes in the CAR file, the slot
    /// recorded within the reward data must match the slot being processed.
    #[error("reward slot mismatch: expected {expected}, found {found}")]
    RewardSlotMismatch { expected: u64, found: u64 },

    /// Failed to decompress data using Zstd.
    ///
    /// CAR files and Solana data structures may be compressed with Zstd
    /// for storage efficiency. This error occurs when the decompression
    /// process fails, indicating corrupted or invalid compressed data.
    #[error("Zstd decompression failed for {field_name}: {error}")]
    Zstd {
        field_name: &'static str,
        error: String,
    },

    /// Failed to deserialize data using bincode.
    ///
    /// Some Solana data structures in CAR files are serialized with bincode.
    /// This error occurs when the binary data cannot be deserialized, indicating
    /// data corruption or format version mismatch.
    #[error("bincode deserialization failed")]
    Bincode(#[source] bincode::Error),

    /// Failed to decode OF1 CAR file field using both prost and bincode.
    ///
    /// Some fields in CAR files can be encoded in different formats. The
    /// extractor attempts both prost (protobuf) and bincode deserialization.
    /// This error occurs when both attempts fail, with error details from
    /// each attempt.
    #[error("failed to decode {field_name}: prost_err={prost_err}, bincode_err={bincode_err}")]
    DecodeField {
        field_name: &'static str,
        prost_err: String,
        bincode_err: String,
    },

    /// Failed to parse a CAR node.
    ///
    /// This occurs during low-level parsing of CAR node structures, indicating
    /// malformed or corrupted node data that cannot be interpreted.
    #[error("CAR node parsing error")]
    NodeParse(#[source] NodeError),

    /// Failed to reassemble a dataframe from CAR nodes.
    ///
    /// Large data structures in CAR files may be split across multiple nodes.
    /// This error occurs when the reassembly of these fragmented structures fails,
    /// typically due to missing or corrupted fragment nodes.
    #[error("CAR dataframe reassembly error")]
    DataframeReassembly(#[source] ReassableError),
}

/// Error during Solana extractor initialization or operation.
#[derive(Debug, thiserror::Error)]
#[error("Extractor error: {0}")]
pub struct ExtractorError(pub String);
