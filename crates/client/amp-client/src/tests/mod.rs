//! Tests for the streaming client

#[macro_use]
mod utils;

mod it_blockchain_reorg_test;
mod it_happy_path_test;
#[cfg(feature = "lmdb")]
mod it_lmdb_store_test;
#[cfg(feature = "postgres")]
mod it_postgres_store;
mod it_rewind_and_crash_recovery_test;
