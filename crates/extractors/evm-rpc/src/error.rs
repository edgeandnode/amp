use alloy::primitives::ruint::FromUintError;
use datasets_raw::rows::TableRowError;
use tokio::sync::AcquireError;

/// Errors that occur during batched RPC request execution.
///
/// The EVM RPC extractor batches multiple block requests together for efficiency.
/// These errors cover failures in the batching infrastructure, including transport
/// errors and rate limiting issues.
#[derive(thiserror::Error, Debug)]
pub enum BatchingError {
    /// The RPC batch request failed.
    ///
    /// This occurs when the underlying transport (HTTP, WebSocket, IPC) encounters
    /// an error while sending the batch request or receiving the response. Common
    /// causes include network timeouts, connection drops, or RPC node errors.
    #[error("RPC batch request failed: {0}")]
    Request(#[source] BatchRequestError),

    /// Failed to acquire a permit from the rate limiter.
    ///
    /// The extractor uses a semaphore-based rate limiter to prevent overwhelming
    /// the RPC endpoint. This error occurs when the semaphore is closed, typically
    /// indicating the extractor is shutting down.
    #[error("rate limiter semaphore closed: {0}")]
    RateLimitAcquire(#[source] AcquireError),
}

/// Error wrapper for RPC transport failures.
///
/// This wraps the underlying Alloy transport error that occurs during RPC
/// communication. It provides a consistent error type for batch request failures
/// while preserving the original transport error details.
#[derive(thiserror::Error, Debug)]
#[error("RPC client error")]
pub struct BatchRequestError(#[source] pub alloy::transports::TransportError);

/// Errors that occur when converting RPC responses to table rows.
///
/// After fetching block data from the RPC, it must be transformed into the
/// tabular format used for storage. These errors cover data consistency issues
/// and conversion failures during that transformation.
#[derive(Debug, thiserror::Error)]
pub enum RpcToRowsError {
    /// Transaction and receipt counts don't match for a block.
    ///
    /// Each transaction should have exactly one receipt. This error occurs when
    /// the RPC returns a different number of transactions than receipts for a block,
    /// indicating data corruption or an RPC node bug.
    #[error(
        "mismatched tx and receipt count for block {block_num}: {tx_count} txs, {receipt_count} receipts"
    )]
    TxReceiptCountMismatch {
        block_num: u64,
        tx_count: usize,
        receipt_count: usize,
    },

    /// Transaction and receipt hashes don't match.
    ///
    /// When pairing transactions with their receipts, the transaction hashes must
    /// match. This error occurs when a receipt's transaction hash differs from the
    /// corresponding transaction, indicating ordering issues or data corruption.
    #[error(
        "mismatched tx and receipt hash for block {block_num}: tx {tx_hash}, receipt {receipt_hash}"
    )]
    TxReceiptHashMismatch {
        block_num: u64,
        tx_hash: String,
        receipt_hash: String,
    },

    /// Failed to convert RPC data to row format.
    ///
    /// This occurs when individual field conversion fails, such as missing required
    /// fields or numeric overflow during type conversion.
    #[error("row conversion failed")]
    ToRow(#[source] ToRowError),

    /// Failed to build the final table rows.
    ///
    /// This occurs when the Arrow table builder fails to construct the final
    /// record batch from the converted rows, typically due to schema issues.
    #[error("table build failed")]
    TableRow(#[source] TableRowError),
}

/// Errors during individual field conversion to row format.
///
/// When converting RPC response fields to the storage format, each field must
/// be validated and potentially converted to a different type. These errors
/// indicate specific field-level failures.
#[derive(thiserror::Error, Debug)]
pub enum ToRowError {
    /// A required field is missing from the RPC response.
    ///
    /// Certain fields are mandatory for row construction (e.g., block number,
    /// transaction hash). This error occurs when the RPC response lacks a
    /// required field, which may indicate an incomplete response or API change.
    #[error("missing field: {0}")]
    Missing(&'static str),

    /// A numeric field overflowed during type conversion.
    ///
    /// RPC responses may contain large numbers (U256 for gas, value, etc.) that
    /// must be converted to smaller storage types. This error occurs when a value
    /// exceeds the target type's range.
    #[error("overflow in field {0}: {1}")]
    Overflow(&'static str, #[source] OverflowSource),
}

/// Source of numeric overflow errors during field conversion.
///
/// Different numeric types have different conversion error types. This enum
/// unifies them to provide consistent error handling regardless of the
/// source numeric type.
#[derive(Debug, thiserror::Error)]
pub enum OverflowSource {
    /// Overflow from standard integer type conversion.
    ///
    /// This occurs when converting between Rust integer types (e.g., u64 to i64,
    /// u128 to u64) and the source value exceeds the target type's range.
    #[error("{0}")]
    Int(#[source] std::num::TryFromIntError),

    /// Overflow from big integer (U256) conversion.
    ///
    /// This occurs when converting Ethereum's 256-bit unsigned integers to smaller
    /// types like i128. Large values (e.g., token amounts, gas prices) may exceed
    /// the target type's maximum value.
    #[error("{0}")]
    BigInt(#[source] FromUintError<i128>),
}

/// Error connecting to an EVM RPC provider.
///
/// This error wraps the underlying Alloy transport error that occurs when
/// establishing a connection to an RPC endpoint. Common causes include
/// invalid URLs, network connectivity issues, or authentication failures.
#[derive(thiserror::Error, Debug)]
#[error("provider error: {0}")]
pub struct ProviderError(#[source] pub alloy::transports::TransportError);
