//! Internal database connection abstractions
//!
//! This module provides the core database primitives including connection pools
//! and transactions.
//!
//! The module is private to the crate - only selected types are re-exported
//! publicly through lib.rs.

mod conn;
mod exec;
mod txn;

pub use conn::{ConnError, ConnPool, Connection};
pub use exec::Executor;
pub use txn::Transaction;
