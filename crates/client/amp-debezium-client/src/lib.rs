//! A Debezium CDC-compliant streaming client for Amp, enabling integration with
//! stream processing engines like Arroyo.
//!
//! This crate wraps Amp's Arrow Flight streaming client and transforms query results
//! into Debezium format with proper blockchain reorg handling.
//!
//! ## Example
//!
//! ```no_run
//! use amp_debezium::{DebeziumClient, DebeziumOp, InMemoryStore};
//! use futures::StreamExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a Debezium client
//!     let client = DebeziumClient::builder()
//!         .amp_endpoint("http://localhost:1602")?
//!         .primary_keys(vec!["block_num".to_string(), "log_index".to_string()])
//!         .reorg_window(64) // Keep last 64 blocks for reorg detection
//!         .state_store(InMemoryStore::new(64))
//!         .build()
//!         .await?;
//!
//!     // Execute a streaming query
//!     let mut stream = client
//!         .stream(
//!             "SELECT * FROM eth_rpc.logs WHERE address = '0x...' SETTINGS stream = true",
//!             None,
//!         )
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
//! 2. All records within the invalidated block range are retrieved from the state store
//! 3. Delete events are emitted for each invalidated record
//! 4. Subsequent batches will contain the new canonical data
//!
//! This ensures downstream systems can properly handle chain reorgs by retracting
//! orphaned data before processing the new canonical chain.

pub mod client;
pub mod error;
pub mod primary_key;
pub mod state;
pub mod types;

// Re-export main types
pub use client::{DebeziumClient, DebeziumClientBuilder};
pub use error::{Error, Result};
pub use primary_key::PrimaryKeyExtractor;
pub use state::{InMemoryStore, StateStore};
pub use types::{DebeziumOp, DebeziumRecord, RecordKey, StoredRecord};
