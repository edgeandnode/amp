//! GC manifest management module for metadata database
//!
//! This module provides functionality for managing the garbage collection manifest,
//! which tracks files scheduled for deletion along with their expiration times.

use std::time::Duration;

use futures::stream::{BoxStream, StreamExt};

use crate::{Error, Executor, files::FileId, physical_table::LocationId};

pub(crate) mod sql;

pub use sql::GcManifestRow;

/// Schedules files for deletion by upserting GC manifest entries.
/// Updates expiration time if file already exists in manifest.
#[tracing::instrument(skip(exe), err)]
pub async fn upsert<'c, E>(
    exe: E,
    location_id: LocationId,
    file_ids: &[FileId],
    duration: Duration,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::upsert(exe, location_id, file_ids, duration)
        .await
        .map_err(Error::Database)
}

/// Streams files that have passed their expiration time and are ready for deletion.
#[tracing::instrument(skip(exe))]
pub fn stream_expired<'c, E>(
    exe: E,
    location_id: LocationId,
) -> BoxStream<'c, Result<GcManifestRow, Error>>
where
    E: Executor<'c> + 'c,
{
    sql::stream_expired(exe, location_id)
        .map(|r| r.map_err(Error::Database))
        .boxed()
}
