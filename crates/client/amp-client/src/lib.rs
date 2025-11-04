//! Rust client library for Amp
//!
//! This crate provides a high-level client for querying blockchain data from Amp servers.
//!
//! # Quick Start
//!
//! ## Simple Query
//!
//! ```rust,ignore
//! use amp_client::AmpClient;
//! use futures::StreamExt;
//!
//! // Connect to server
//! let mut client = AmpClient::from_endpoint("http://localhost:1602").await?;
//!
//! // Execute non-streaming query
//! let mut result = client.query("SELECT * FROM eth.blocks LIMIT 10").await?;
//! while let Some(batch) = result.next().await {
//!     // Process batch
//! }
//! ```
//!
//! ## Protocol Stream (Stateless Reorg Detection)
//!
//! ```rust,ignore
//! use amp_client::{AmpClient, ProtocolMessage};
//! use futures::StreamExt;
//!
//! let client = AmpClient::from_endpoint("http://localhost:1602").await?;
//!
//! // Create protocol stream (default - stateless reorg detection)
//! let mut stream = client
//!     .stream("SELECT * FROM eth.logs WHERE address = '0x...' SETTINGS stream = true")
//!     .await?;
//!
//! while let Some(msg) = stream.next().await {
//!     match msg? {
//!         ProtocolMessage::Data { batch, ranges } => {
//!             // Process new data
//!             println!("Received batch covering blocks: {:?}", ranges);
//!         }
//!         ProtocolMessage::Reorg { previous, incoming, invalidation } => {
//!             // Handle reorg (invalidate data in reorg'd ranges)
//!             println!("Reorg detected! Invalidation: {:?}", invalidation);
//!         }
//!         ProtocolMessage::Watermark { ranges } => {
//!             // Watermark (ranges complete)
//!             println!("Watermark reached: {:?}", ranges);
//!         }
//!     }
//! }
//! ```
//!
//! ## Transactional Stream (Stateful with Exactly-Once Semantics)
//!
//! ```rust,ignore
//! use amp_client::{AmpClient, InMemoryStateStore, TransactionEvent};
//! use futures::StreamExt;
//!
//! let client = AmpClient::from_endpoint("http://localhost:1602").await?;
//!
//! // Create transactional stream with state persistence
//! let mut stream = client
//!     .stream("SELECT * FROM eth.logs WHERE address = '0x...' SETTINGS stream = true")
//!     .transactional(InMemoryStateStore::new(), 128)
//!     .await?;
//!
//! while let Some(result) = stream.next().await {
//!     let (event, commit) = result?;
//!
//!     match event {
//!         TransactionEvent::Data { batch, id, .. } => {
//!             // Process and save data with transaction id
//!             save_data(id, batch).await?;
//!             commit.await?;
//!         }
//!         TransactionEvent::Undo { invalidate, .. } => {
//!             // Rollback invalidated transaction ids (reorg or rewind)
//!             delete_data(invalidate).await?;
//!             commit.await?;
//!         }
//!         _ => {}
//!     }
//! }
//! ```

mod client;
mod decode;
mod error;
pub mod store;
mod transactional;

pub use client::{
    AmpClient, BatchStream, InvalidationRange, Metadata, ProtocolMessage, ProtocolStream,
    RawStream, ResponseBatch, StreamBuilder,
};
pub use common::metadata::segments::BlockRange;
pub use error::Error;
#[cfg(feature = "lmdb")]
pub use store::LmdbStateStore;
pub use store::{InMemoryStateStore, StateSnapshot, StateStore};
pub use transactional::{Cause, CommitHandle, TransactionEvent, TransactionalStream};

#[cfg(test)]
mod tests;
