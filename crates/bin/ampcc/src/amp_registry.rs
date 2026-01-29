//! AMP Registry client and domain types.

mod client;
mod domain;
mod error;

pub use client::AmpRegistryClient;
pub use domain::*;
pub use error::AmpRegistryError;
