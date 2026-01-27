//! Canton Network extractor for AMP.
//!
//! This crate provides data extraction from Canton Network ledgers via canton-bridge.
//! Canton uses ledger offsets instead of block numbers, which are mapped to AMP's
//! BlockNum type for compatibility.
//!
//! ## Architecture
//!
//! ```text
//! AMP (this crate)
//!   │
//!   │ BlockStreamer trait (offsets mapped to BlockNum)
//!   ▼
//! canton-bridge (Arrow Flight)
//!   │
//!   │ gRPC (Canton Ledger API)
//!   ▼
//! Canton Validator
//! ```
//!
//! ## Tables
//!
//! - `transactions` - Transaction updates from the ledger
//! - `contracts_created` - Contract creation events
//! - `contracts_archived` - Contract archival events

pub mod client;
pub mod dataset;
mod dataset_kind;
pub mod tables;

pub use self::{
    client::Client,
    dataset::{Dataset, Manifest, ProviderConfig, dataset},
    dataset_kind::{CantonDatasetKind, CantonDatasetKindError},
};

/// Errors that can occur when working with Canton data sources.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Connection error to canton-bridge
    #[error("Connection error: {0}")]
    Connection(#[source] client::Error),
    /// Stream error during data extraction
    #[error("Stream error: {0}")]
    Stream(String),
}
