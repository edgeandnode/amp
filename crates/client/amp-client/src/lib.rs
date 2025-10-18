//! Rust client library for Amp
//!
//! This crate provides a high-level client for querying blockchain data from Amp servers.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use amp_client::{AmpClient, Event};
//! use futures::StreamExt;
//!
//! // Connect to server
//! let client = AmpClient::from_endpoint("http://localhost:1602").await?;
//!
//! // Simple query
//! let mut result = client.query("SELECT * FROM eth.blocks LIMIT 10").await?;
//! while let Some(batch) = result.next().await {
//!     // Process batch
//! }
//!
//! // Streaming query with reorg detection
//! let mut stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true").await?;
//!
//! while let Some(result) = stream.next().await {
//!     let (event, commit) = result?;
//!     match event {
//!         Event::Data { batch, id, ranges, .. } => {
//!             // Process data
//!             save_data(id, batch).await?;
//!             commit.await?;
//!         }
//!         Event::Reorg { invalidation, .. } => {
//!             // Handle reorg
//!             rollback_ids(invalidation).await?;
//!             commit.await?;
//!         }
//!         Event::Rewind { invalidation, .. } => {
//!             // Handle rewind
//!             rollback_ids(invalidation).await?;
//!             commit.await?;
//!         }
//!         Event::Watermark { id, ranges, cutoff, .. } => {
//!             // Handle watermark
//!             save_checkpoint(id, ranges).await?;
//!             if let Some(cutoff) = cutoff {
//!                 prune_old_data(cutoff).await?;
//!             }
//!             commit.await?;
//!         }
//!     }
//! }
//! ```

mod client;
mod decode;
mod error;
mod state;
pub mod store;
mod stream;

pub use client::{AmpClient, BatchStream, Metadata, RawStream, ResponseBatch};
pub use common::metadata::segments::BlockRange;
pub use error::Error;
pub use state::CommitHandle;
pub use store::{InMemoryStateStore, StateStore, StreamState};
pub use stream::{Event, Stream, StreamBuilder, WatermarkCheckpoint};
