//! A Debezium CDC-compliant streaming client for Amp, enabling integration with
//! stream processing engines like Arroyo.
//!
//! This crate wraps Amp's Arrow Flight streaming client and transforms query results
//! into Debezium format with proper blockchain reorg handling.
//!
//! ## Example
//!
//! ```no_run
//! use amp_client::AmpClient;
//! use amp_debezium_client::{DebeziumClient, DebeziumOp};
//! use futures::StreamExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a Debezium client (defaults to InMemoryStore with 64-block reorg window)
//!     let amp_client = AmpClient::from_endpoint("http://localhost:1602").await?;
//!     let client = DebeziumClient::new(amp_client, None);
//!
//!     // Execute a streaming query
//!     let mut stream = client
//!         .stream("SELECT * FROM eth_rpc.logs WHERE address = '0x...' SETTINGS stream = true")
//!         .await?;
//!
//!     // Process Debezium CDC events
//!     while let Some(record) = stream.next().await {
//!         match record? {
//!             record if record.op == DebeziumOp::Create => {
//!                 println!("New record: {:?}", record.after);
//!             }
//!             record if record.op == DebeziumOp::Delete => {
//!                 println!("Retracted record (reorg): {:?}", record.before);
//!             }
//!             _ => {}
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Debezium Format
//!
//! Each record emitted by the stream follows the Debezium CDC format:
//!
//! ```json
//! {
//!   "before": null,
//!   "after": {
//!     "block_num": 100,
//!     "log_index": 5,
//!     "address": "0x..."
//!   },
//!   "op": "c"
//! }
//! ```
//!
//! Where `op` can be:
//! - `"c"` - Create (new record)
//! - `"u"` - Update (modified record)
//! - `"d"` - Delete (retracted due to reorg)
//!
//! ## Reorg Handling
//!
//! When a blockchain reorganization is detected:
//!
//! 1. The client receives a reorg signal from Amp with invalidation ranges
//! 2. All batches whose ranges overlap the invalidated range are retrieved
//! 3. Delete events are emitted for every record in those batches (batch-level granularity)
//! 4. Subsequent batches will contain the new canonical data
//!
//! **Note:** Batches are treated as atomic units. If a reorg affects any network range
//! within a batch, ALL records in that batch are retracted, even records from other
//! networks or block ranges. This is a conservative approach that ensures correctness.
//!
//! ## Pruning
//!
//! Batches are pruned based on watermarks for each network:
//!
//! - Each network has its own watermark tracking progress
//! - A batch is deleted only when ALL its ranges are beyond their network's reorg window
//! - If even one network's range is still within its reorg window, the entire batch is kept
//!
//! This multi-network aware pruning ensures data is retained long enough for all chains,
//! preventing premature deletion of data needed for slower chains.
//!
//! ## Storage Backends
//!
//! Two state store implementations are available:
//!
//! - **`InMemoryStore`** (default) - Fast, in-memory storage using Vec. Does not persist across restarts.
//! - **`LmdbStore`** (feature = "lmdb") - Persistent disk storage. Survives restarts.
//!
//! Enable LMDB backend with: `cargo add amp-debezium-client --features lmdb`

pub mod client;
pub mod error;
pub mod stores;
pub mod types;

// Re-export main types
pub use client::DebeziumClient;
pub use error::{Error, Result};
#[cfg(feature = "lmdb")]
pub use stores::LmdbStore;
pub use stores::{InMemoryStore, StateStore};
pub use types::{DebeziumOp, DebeziumRecord};
