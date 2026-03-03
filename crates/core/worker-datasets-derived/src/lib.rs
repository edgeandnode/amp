//! # Derived Dataset Materialization
//!
//! Worker job implementation for materializing derived datasets — datasets defined by
//! user-authored SQL queries that transform or aggregate existing blockchain data — into
//! Parquet files.
//!
//! ## Crate Structure
//!
//! - [`job_kind`] — Job kind discriminator (`"materialize-derived"`) used by the worker
//!   dispatcher to route jobs to this implementation.
//! - [`job_descriptor`] — Typed job payload ([`job_descriptor::JobDescriptor`]) carrying
//!   dataset identity, manifest hash, and target block height. Implements edge validation
//!   when converting from raw DB JSON.
//! - [`job_impl`] — Execution pipeline split into three layers:
//!   - [`job_impl::execute`] — Dataset-level orchestrator: fetches the manifest, resolves
//!     physical table revisions, locks them to the job, runs consistency checks, then
//!     spawns parallel per-table tasks via `FailFastJoinSet`.
//!   - [`job_impl::table::materialize_table`] — Per-table orchestrator: parses SQL from
//!     the manifest, resolves dependency datasets to pinned `HashReference`s, builds a
//!     physical catalog and DataFusion planning context, validates the query is
//!     incremental, resolves the block range, and delegates to the query layer.
//!   - [`job_impl::query::materialize_sql_query`] — SQL execution layer: spawns a
//!     `StreamingQuery`, processes the `QueryMessage` stream (data batches, microbatch
//!     boundaries), writes Parquet files, commits metadata, and triggers compaction.
//!
//! ## Job Descriptor
//!
//! A [`job_descriptor::JobDescriptor`] contains:
//!
//! | Field                | Purpose                                                |
//! |----------------------|--------------------------------------------------------|
//! | `end_block`          | Target block height (bounded or continuous)            |
//! | `dataset_namespace`  | Dataset identity (namespace)                           |
//! | `dataset_name`       | Dataset identity (name)                                |
//! | `manifest_hash`      | Exact manifest version pinning SQL/schema definitions  |
//!
//! The descriptor validates at the DB boundary via `TryFrom<&JobDescriptorRaw>`, which
//! checks the `"materialize-derived"` kind tag and deserializes fields. Once converted,
//! domain code trusts the descriptor without re-validation.
//!
//! ## Execution Pipeline
//!
//! ```text
//! metadata-db (JobDescriptorRaw)
//!   → JobDescriptor (validated at boundary)
//!     → execute(): fetch manifest, resolve tables, lock revisions, consistency check
//!       → materialize_table() ×N concurrently (FailFastJoinSet)
//!         → parse SQL, resolve deps, build catalog, plan query
//!         → materialize_sql_query()
//!           → StreamingQuery → ParquetFileWriter (per microbatch)
//!             → commit_metadata() → compactor
//! ```
//!
//! **Concurrency**: tables within a dataset are materialized in parallel. If any table
//! fails, all sibling tasks are aborted (fail-fast) to prevent partial materializations.
//!
//! **Progress**: sync lifecycle events (`SyncStarted`, `SyncCompleted`, `SyncFailed`) are
//! emitted per table. Incremental progress updates are time-throttled to avoid flooding
//! consumers.

pub mod job_descriptor;
pub mod job_impl;
pub mod job_kind;
