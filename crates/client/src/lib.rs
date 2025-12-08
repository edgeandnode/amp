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
//!     .stream("SELECT * FROM eth.logs WHERE address = '0x...'")
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
//! ## Transactional Stream (Stateful)
//!
//! ```rust,ignore
//! use amp_client::{AmpClient, InMemoryStateStore, TransactionEvent};
//! use futures::StreamExt;
//!
//! let client = AmpClient::from_endpoint("http://localhost:1602").await?;
//!
//! // Create transactional stream with state persistence
//! let mut stream = client
//!     .stream("SELECT * FROM eth.logs WHERE address = '0x...'")
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
//!
//! ## CDC Stream (Change Data Capture)
//!
//! ```rust,ignore
//! use amp_client::{AmpClient, CdcEvent};
//! use amp_client::store::{open_lmdb_env, LmdbStateStore, LmdbBatchStore};
//! use futures::StreamExt;
//!
//! let env = open_lmdb_env("/path/to/db")?;
//! let state_store = LmdbStateStore::new(env.clone())?;
//! let batch_store = LmdbBatchStore::new(env)?;
//!
//! let client = AmpClient::from_endpoint("http://localhost:1602").await?;
//! let mut stream = client
//!     .stream("SELECT * FROM eth.logs WHERE address = '0x...'")
//!     .cdc(state_store, batch_store, 128)
//!     .await?;
//!
//! while let Some(result) = stream.next().await {
//!     let (event, commit) = result?;
//!
//!     match event {
//!         CdcEvent::Insert { batch, .. } => {
//!             for row in batch.rows() {
//!                 // Process the row
//!             }
//!             commit.await?;
//!         }
//!         CdcEvent::Delete { mut batches, .. } => {
//!             while let Some(result) = batches.next().await {
//!                 let (id, batch) = result?;
//!                 for row in batch.rows() {
//!                     // Process the row
//!                 }
//!             }
//!             commit.await?;
//!         }
//!     }
//! }
//! ```

use std::{collections::BTreeMap, ops::RangeInclusive};

use alloy::primitives::BlockHash;

mod cdc;
mod client;
mod decode;
mod error;
pub mod store;
#[cfg(test)]
mod tests;
mod transactional;
mod validation;

pub use cdc::{CdcEvent, CdcStream, DeleteBatchIterator};
pub use client::{
    AmpClient, BatchStream, HasSchema, InvalidationRange, Metadata, ProtocolMessage,
    ProtocolStream, RawStream, ResponseBatch, StreamBuilder,
};
pub use error::Error;
#[cfg(feature = "postgres")]
pub use store::PostgresStateStore;
pub use store::{BatchStore, InMemoryBatchStore, InMemoryStateStore, StateSnapshot, StateStore};
#[cfg(feature = "lmdb")]
pub use store::{LmdbBatchStore, LmdbStateStore};
pub use transactional::{Cause, CommitHandle, TransactionEvent, TransactionalStream};

pub type BlockNum = u64;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BlockRange {
    pub numbers: RangeInclusive<BlockNum>,
    pub network: String,
    pub hash: BlockHash,
    pub prev_hash: Option<BlockHash>,
}

impl BlockRange {
    #[inline]
    pub fn start(&self) -> BlockNum {
        *self.numbers.start()
    }

    #[inline]
    pub fn end(&self) -> BlockNum {
        *self.numbers.end()
    }

    #[inline]
    pub fn watermark(&self) -> Watermark {
        Watermark {
            number: self.end(),
            hash: self.hash,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Watermark {
    /// The segment end block
    pub number: BlockNum,
    /// The hash associated with the segment end block
    pub hash: BlockHash,
}

/// Public interface for resuming a stream from a watermark.
// TODO: unify with `Watermark` when adding support for multi-network streaming.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResumeWatermark(pub BTreeMap<String, Watermark>);

impl ResumeWatermark {
    pub fn from_ranges(ranges: &[BlockRange]) -> Self {
        let watermark = ranges
            .iter()
            .map(|r| {
                let watermark = r.watermark();
                (r.network.clone(), watermark)
            })
            .collect();
        Self(watermark)
    }

    pub fn to_watermark(
        self,
        network: &str,
    ) -> Result<Watermark, Box<dyn std::error::Error + Sync + Send + 'static>> {
        self.0
            .into_iter()
            .find(|(n, _)| n == network)
            .map(|(_, w)| w)
            .ok_or_else(|| format!("Expected resume watermark for network '{network}'").into())
    }
}
