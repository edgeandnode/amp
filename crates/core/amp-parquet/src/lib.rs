//! Parquet file I/O for the Amp pipeline.
//!
//! This crate consolidates all Parquet-related operations: reading files via DataFusion's
//! async reader interface ([`reader`]), writing new Parquet segments ([`writer`]),
//! extracting and committing file metadata ([`footer`], [`commit`]), and defining the
//! Amp-specific metadata embedded in every Parquet file ([`meta`]).
//!
//! It also provides the [`retry::RetryableErrorExt`] trait used to classify I/O errors
//! as transient or fatal so callers can decide whether to retry.

pub mod commit;
pub mod footer;
pub mod generation;
pub mod meta;
pub mod reader;
pub mod retry;
pub mod timestamp;
pub mod writer;
