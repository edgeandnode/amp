//! Raw dataset utilities and schema generation support.
//!
//! This crate provides utilities for working with raw datasets, including
//! build-time schema generation for documentation purposes.

pub mod client;
pub mod rows;
#[cfg(feature = "gen-schema")]
pub mod schema;
