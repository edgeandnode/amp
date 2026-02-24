//! Physical table — segment resolution and table identity.
//!
//! This module owns the concept of segments and physical table resolution.
//! The query execution layer becomes agnostic to segments — it receives resolved
//! file lists and table metadata without knowing how they were derived.
//!
//! ## Module layout
//!
//! - `file` — `FileMetadata`: file identity and metadata from DB rows
//! - `segments` — `Segment`, `Chain`, `canonical_chain()`, `missing_ranges()`
//! - `resolved` — `ResolvedFile`: resolved file view for execution layer
//! - `table` — `PhysicalTable`: identity + segment resolution
//! - `snapshot` — `TableSnapshot`: resolved segments + resolved files view

pub mod file;
pub mod resolved;
pub mod segments;
pub mod snapshot;
pub mod table;

pub use file::FileMetadata;
pub use snapshot::MultiNetworkSegmentsError;
pub use table::{
    CanonicalChainError, GetFilesError, GetSegmentsError, MissingRangesError, PhysicalTable,
    SnapshotError,
};
