//! Custom executor trait for metadata database operations
//!
//! This module defines a marker trait that extends [`sqlx::Executor`] and restricts
//! which types can be used as database executors in the public API.

use sqlx::Postgres;

/// Database executor trait that extends [`sqlx::Executor`]
///
/// This trait acts as a marker to restrict which types can be used
/// as executors in the public API, while still providing full
/// [`sqlx::Executor`] functionality through the trait bound.
pub trait Executor<'c>: sqlx::Executor<'c, Database = Postgres> + crate::_priv::Sealed {}
