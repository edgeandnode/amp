use amp_providers_common::network_id::NetworkId;
use datasets_raw::{client::BlockStreamError, rows::TableRowError};
use yellowstone_faithful_car_parser as car_parser;

use crate::of1_client;

pub type SlotConversionResult<T> = Result<T, SlotConversionError>;
pub type RowConversionResult<T> = Result<T, RowConversionError>;

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

    /// Failed to decode a base58-encoded return data field.
    ///
    /// Some return data fields in Solana transactions may be encoded as base58 strings.
    /// This error occurs when such a field cannot be decoded, indicating invalid data or
    /// unexpected formatting.
    #[error("failed to decode base58 return data at slot {slot}, tx index {tx_idx}")]
    DecodeReturnData { slot: u64, tx_idx: usize },

    /// Failed to decode a base64-encoded instruction data field.
    ///
    /// Some instruction data fields in Solana transactions may be encoded as base64 strings.
    /// This error occurs when such a field cannot be decoded, indicating invalid data or
    /// unexpected formatting.
    #[error("failed to decode base64 instruction data at slot {slot}, tx index {tx_idx}")]
    DecodeInstructionData { slot: u64, tx_idx: usize },

    /// Failed to serialize a transaction error for storage.
    ///
    /// When a transaction error occurs, the extractor attempts to serialize the error details
    /// for storage in the database. This error occurs when the serialization process fails,
    /// typically due to issues with the error data structure or unexpected content.
    #[error("failed to serialize transaction error at slot {slot}, tx index {tx_idx}")]
    SerializeTransactionError {
        slot: u64,
        tx_idx: usize,
        #[source]
        source: serde_json::Error,
    },

    /// Failed to deserialize a transaction error from proto transaction metadata.
    ///
    /// When processing transaction metadata from the CAR files, the extractor may encounter
    /// error details that need to be deserialized from a binary format. This error occurs when
    /// the deserialization process fails, indicating corrupted data or format mismatches.
    #[error("failed to deserialize transaction error at slot {slot}, tx index {tx_idx}")]
    DeserializeTransactionError {
        slot: u64,
        tx_idx: usize,
        #[source]
        source: bincode::Error,
    },

    /// Could not convert a proto reward type integer to the corresponding reward type enum
    /// variant.
    ///
    /// The Solana protobuf definitions represent reward types as integers, but the extractor
    /// uses a Rust enum for better type safety. This error occurs when the integer value from
    /// the proto does not match any known reward type variant, indicating an unexpected or new
    /// reward type that the extractor does not recognize.
    #[error("invalid reward type value {0} in proto")]
    InvalidRewardType(i32),

    /// Could not convert a proto reward commission string to a u8 percentage value.
    ///
    /// The Solana protobuf definitions represent reward commissions as strings, but the extractor
    /// expects them to be parseable as u8 integers. This error occurs when the commission string
    /// cannot be parsed as a valid u8 value, indicating invalid data or formatting issues.
    #[error("invalid reward commission value '{0}' in proto")]
    InvalidRewardComission(String),
}

/// Errors that occur during conversion of Solana block data to the internal `NonEmptySlot`
/// representation.
///
/// These errors indicate issues during the initial processing of raw Solana block data (from RPC
/// or CAR files) into the `NonEmptySlot` struct, which is the intermediate representation before
/// converting to table rows.
#[derive(Debug, thiserror::Error)]
pub enum SlotConversionError {
    /// There were no transactions in the confirmed block.
    ///
    /// This occurs when the extractor retrieves a confirmed block via JSON-RPC and finds that the
    /// transactions array is missing, which should not be the case. This may indicate an error in
    /// the JSON-RPC request configuration or an unexpected response from the RPC node.
    #[error("missing transactions in confirmed block {slot}")]
    MissingTransactions { slot: u64 },

    /// There were no block rewards in the confirmed block.
    ///
    /// This occurs when the extractor retrieves a confirmed block from the RPC or CAR file and finds
    /// that the block rewards array is missing, which should not be the case. This may indicate an
    /// error in the request configuration or an unexpected response from the RPC node or CAR file
    /// structure.
    #[error("missing block rewards in confirmed block {slot}")]
    MissingBlockRewards { slot: u64 },

    /// Unexpected transaction encoding format in the confirmed block.
    ///
    /// This occurs when the extractor retrieves a confirmed block and finds that a transaction uses
    /// an encoding format that is not supported or expected by the extractor. This may indicate a
    /// change in the RPC response format, an unsupported transaction version, etc.
    #[error(
        "unexpected transaction encoding at slot {slot}, tx index {tx_index}: expected {expected}, found {found}"
    )]
    UnexpectedTransactionEncoding {
        slot: u64,
        tx_index: usize,
        expected: &'static str,
        found: &'static str,
    },

    /// Unexpected message encoding format in the confirmed block's transactions.
    ///
    /// This occurs when the extractor processes the transactions in a confirmed block and finds that a
    /// transaction message uses an encoding format that is not supported or expected by the extractor.
    /// This may indicate a change in the RPC response format, an unsupported message version, etc.
    #[error(
        "expected raw message for slot {slot}, tx index {tx_index}: expected {expected}, found {found}"
    )]
    UnexpectedMessageEncoding {
        slot: u64,
        tx_index: usize,
        expected: &'static str,
        found: &'static str,
    },

    /// Transaction version is missing from the confirmed block's transactions.
    ///
    /// This occurs when the extractor processes the transactions in a confirmed block and finds that a
    /// transaction is missing the version field, which is part of the database schema. This may indicate
    /// a change in the RPC response format, an unsupported transaction version, etc.
    #[error("missing transaction version for slot {slot}, tx index {tx_index}")]
    MissingTransactionVersion { slot: u64, tx_index: usize },

    /// Unsupported transaction version number in the confirmed block's transactions.
    ///
    /// This occurs when the extractor processes the transactions in a confirmed block and finds that a
    /// transaction has a version number that is not supported by the extractor. This may indicate a
    /// change in the RPC response format, a new transaction version that the extractor does not yet
    /// support, etc.
    #[error(
        "unsupported transaction version number {version} at slot {slot}, transaction index {tx_index}"
    )]
    UnsupportedTransactionVersion {
        version: u8,
        slot: u64,
        tx_index: usize,
    },

    /// Failed to decode a base58-encoded blockhash string from the confirmed block.
    ///
    /// This occurs when the extractor processes the blockhash fields in a confirmed block and finds that a
    /// blockhash string that is expected to be base58-encoded cannot be decoded, indicating invalid data or
    /// unexpected formatting in the blockhash fields of the RPC response or CAR file.
    #[error("failed to decode base58 blockhash for slot {slot}: {encoded}")]
    DecodeBlockhash { slot: u64, encoded: String },

    /// Decoded blockhash has an unexpected length that does not match the expected 32 bytes.
    ///
    /// This occurs when the extractor successfully decodes a blockhash string from base58 but finds that the
    /// resulting byte array does not have the expected length of 32 bytes, which is required for valid Solana
    /// blockhashes. This may indicate invalid data or unexpected formatting in the blockhash fields of the RPC
    /// response or CAR file.
    #[error(
        "block hash for slot {slot} has unexpected length: expected 32 bytes, found {actual_len} bytes"
    )]
    InvalidBlockhashLength { slot: u64, actual_len: usize },

    /// Failed to parse a transaction signature from the confirmed block.
    ///
    /// This occurs when the extractor processes the transactions in a confirmed block and finds that a
    /// transaction signature string cannot be parsed, indicating invalid data or unexpected formatting in the
    /// transaction signatures of the RPC response or CAR file.
    #[error("parsing transaction signature {tx_id}")]
    ParsingTransactionSignature { slot: u64, tx_id: String },

    /// Failed to fetch transaction details for a transaction in the confirmed block.
    ///
    /// This occurs when the extractor attempts to retrieve additional details for a transaction via JSON-RPC
    /// (e.g., using `getTransaction`) and encounters an error, such as network issues, RPC node unavailability,
    /// or unexpected RPC response formats. This may indicate issues with the RPC communication or changes in the
    /// JSON-RPC API.
    #[error("fetching transaction details for tx {tx_id} in slot {slot}: {source}")]
    FetchingTransactionDetails {
        slot: u64,
        tx_id: String,
        #[source]
        source: solana_rpc_client_api::client_error::Error,
    },

    /// Failed to convert a Solana block's transactions, messages, or rewards into table rows.
    ///
    /// This occurs when the extractor has successfully retrieved and processed a confirmed block's data
    /// but encounters an error during the conversion of that data into the tabular format used for storage.
    /// This may indicate issues with the data structure, schema mismatches, or other problems during the
    /// transformation process.
    #[error("failed to convert slot {slot} data into table rows")]
    RowConversion {
        slot: u64,
        #[source]
        source: RowConversionError,
    },
}

impl SlotConversionError {
    /// Helper method to create a [SlotConversionError::RowConversion] from a [RowConversionError].
    pub fn row_conversion_error(slot: u64, source: RowConversionError) -> Self {
        Self::RowConversion { slot, source }
    }
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

    /// Failed to stream a CAR file through the OF1 client.
    ///
    /// This occurs when the OF1 client encounters issues while reading or streaming
    /// CAR files, which may include HTTP errors, unsupported range requests, or
    /// other file access problems.
    #[error("failed to stream CAR file")]
    FileStream(#[source] of1_client::CarReaderError),

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
    ///
    /// This error variant also includes ZSTD decompressed data for debugging
    /// (or other) purposes.
    #[error("failed to decode {field_name}: prost_err={prost_err}, bincode_err={bincode_err}")]
    DecodeField {
        field_name: &'static str,
        decompressed_data: Vec<u8>,
        prost_err: String,
        bincode_err: String,
    },

    /// Failed to parse a CAR node.
    ///
    /// This occurs during low-level parsing of CAR node structures, indicating
    /// malformed or corrupted node data that cannot be interpreted.
    #[error("CAR node parsing error")]
    NodeParse(#[source] car_parser::node::NodeError),

    /// Failed to reassemble a dataframe from CAR nodes.
    ///
    /// Large data structures in CAR files may be split across multiple nodes.
    /// This error occurs when the reassembly of these fragmented structures fails,
    /// typically due to missing or corrupted fragment nodes.
    #[error("CAR dataframe reassembly error")]
    DataframeReassembly(#[source] car_parser::node::ReassableError),

    /// Failed to decode a Base58 string.
    ///
    /// Base58 encoding is commonly used in Solana for addresses and other identifiers.
    /// This error occurs when a string that is expected to be Base58-encoded cannot be
    /// decoded, indicating invalid input data.
    #[error("failed to decode Base58 string {0}")]
    DecodeBase58(#[source] bs58::decode::Error),

    /// Failed to convert a byte buffer to an array of fixed size.
    ///
    /// This error occurs when attempting to convert a byte buffer (for example a
    /// `Vec<u8>` or slice) into an array of a specific size (e.g., for fixed-size
    /// fields in Solana data structures) and the input does not have the expected
    /// length.
    #[error(
        "failed to convert slice to array: expected length {expected_len}, actual length {actual_len}"
    )]
    TryIntoArray {
        expected_len: usize,
        actual_len: usize,
    },

    /// Blocktime value overflowed when converting from u64 to i64.
    ///
    /// Solana blocktimes are represented as i64 but when decoding from CAR files or
    /// other sources, they may initially be in u64 format. This error occurs when the
    /// blocktime value exceeds the maximum value of i64, which prevents safe conversion.
    #[error("blocktime overflow: slot {slot} has blocktime {blocktime} which exceeds i64::MAX")]
    BlocktimeOverflow { slot: u64, blocktime: u64 },
}

impl From<Of1StreamError> for BlockStreamError {
    fn from(value: Of1StreamError) -> Self {
        // There is no catch-all here on purpose, to force consideration of
        // each error type when mapping to recoverable vs fatal.
        match value {
            Of1StreamError::FileStream(of1_client::CarReaderError::Io(_))
            | Of1StreamError::FileStream(of1_client::CarReaderError::Http(
                reqwest::StatusCode::NOT_FOUND,
            ))
            | Of1StreamError::FileStream(of1_client::CarReaderError::RangeRequestUnsupported)
            | Of1StreamError::UnexpectedNode { .. }
            | Of1StreamError::MissingNode { .. }
            | Of1StreamError::RewardSlotMismatch { .. }
            | Of1StreamError::Zstd { .. }
            | Of1StreamError::Bincode(_)
            | Of1StreamError::DecodeField { .. }
            | Of1StreamError::NodeParse(_)
            | Of1StreamError::DataframeReassembly(_)
            | Of1StreamError::DecodeBase58(_)
            | Of1StreamError::TryIntoArray { .. }
            | Of1StreamError::BlocktimeOverflow { .. } => BlockStreamError::Fatal(value.into()),

            Of1StreamError::RpcClient(_)
            | Of1StreamError::FileStream(of1_client::CarReaderError::Http(_))
            | Of1StreamError::FileStream(of1_client::CarReaderError::Reqwest(_)) => {
                BlockStreamError::Recoverable(value.into())
            }
        }
    }
}

/// Error during Solana client initialization or operation.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The requested Solana network is not supported.
    ///
    /// This occurs when the configured Solana network identifier does not match
    /// the only supported network ('mainnet').
    #[error("unsupported Solana network: {network}. Only 'mainnet' is supported.")]
    UnsupportedNetwork { network: NetworkId },

    /// The RPC provider URL uses an unsupported scheme.
    ///
    /// This occurs when the RPC provider URL uses a scheme other than
    /// `http` or `https`.
    #[error("unsupported Solana RPC provider URL scheme: {scheme}")]
    UnsupportedUrlScheme { scheme: String },
}

impl amp_providers_common::retryable::RetryableErrorExt for ClientError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::UnsupportedNetwork { .. } => false,
            Self::UnsupportedUrlScheme { .. } => false,
        }
    }
}
