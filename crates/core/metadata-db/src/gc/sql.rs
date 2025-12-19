//! Internal SQL operations for GC manifest management
//!
//! This module contains the low-level database operations for the garbage collection manifest.
//! Functions in this module return `sqlx::Error` directly and should be wrapped
//! by the public API functions in the parent module.

use std::time::Duration;

use futures::stream::BoxStream;
use sqlx::{Executor, Postgres, postgres::types::PgInterval, types::chrono::NaiveDateTime};

use crate::{files::FileId, physical_table::LocationId};

/// Upsert GC manifest entries
///
/// Inserts or updates the GC manifest for the given file IDs.
/// If a file ID already exists, it updates the expiration time.
/// The expiration time is set to the current time plus the given duration.
/// If the file ID does not exist, it inserts a new row.
pub async fn upsert<'c, E>(
    exe: E,
    location_id: LocationId,
    file_ids: &[FileId],
    duration: Duration,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let interval = PgInterval {
        microseconds: (duration.as_micros() as u64) as i64,
        ..Default::default()
    };

    let query = indoc::indoc! {r#"
        INSERT INTO gc_manifest (location_id, file_id, file_path, expiration)
        SELECT $1
              , file.id
              , file_metadata.file_name
              , CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + $3
           FROM UNNEST ($2) AS file(id)
     INNER JOIN file_metadata ON file_metadata.id = file.id
    ON CONFLICT (file_id) DO UPDATE SET expiration = EXCLUDED.expiration;
    "#};

    sqlx::query(query)
        .bind(location_id)
        .bind(file_ids)
        .bind(interval)
        .execute(exe)
        .await?;

    Ok(())
}

/// Stream expired files from GC manifest
///
/// Returns files where expiration time has passed (ready for garbage collection).
pub fn stream_expired<'c, E>(
    exe: E,
    location_id: LocationId,
) -> BoxStream<'c, Result<GcManifestRow, sqlx::Error>>
where
    E: Executor<'c, Database = Postgres> + 'c,
{
    let query = indoc::indoc! {r#"
        SELECT location_id
             , file_id
             , file_path
             , expiration
          FROM gc_manifest
         WHERE location_id = $1
               AND expiration < CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
    "#};

    Box::pin(sqlx::query_as(query).bind(location_id).fetch(exe))
}

/// GC manifest row from the database
#[derive(Debug, sqlx::FromRow)]
pub struct GcManifestRow {
    /// gc_manifest.location_id
    pub location_id: LocationId,
    /// gc_manifest.file_id
    pub file_id: FileId,
    /// gc_manifest.file_path
    pub file_path: String,
    /// gc_manifest.expiration
    pub expiration: NaiveDateTime,
}
