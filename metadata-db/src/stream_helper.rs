use std::{
    error::Error,
    pin::Pin,
    task::Poll::{self, Pending, Ready},
};

use futures::{
    future::{self, Ready as FutureReady},
    stream::{BoxStream, StreamExt, TryStreamExt},
    Stream,
};
use object_store::{
    path::Path,
    Error::{self as ObjectStoreError, Generic, InvalidPath},
    ObjectMeta,
};
use serde_json::Value as ScannedRangeJson;
use sqlx::{types::chrono::NaiveDateTime, Error as SqlxError};
use url::Url;

type RangeEnd = i64;
type FileName = String;

type FileMetaRow = (
    RangeEnd,
    FileName,
    String,
    NaiveDateTime,
    i64,
    Option<String>,
    Option<String>,
    ScannedRangeJson,
);
type FileNameRow = (FileName, RangeEnd);
type RangeRow = (i64, RangeEnd);

/// A row from the `file_metadata` table in the [`MetadataDb`].
pub(crate) trait InboundRowType {
    fn range_end(&self) -> RangeEnd;
    fn is_overlapping<E>(curr: RangeEnd) -> impl FnMut(&Self) -> FutureReady<Result<bool, E>> {
        move |this| future::ok(this.range_end() <= curr)
    }
}

impl InboundRowType for FileMetaRow {
    fn range_end(&self) -> RangeEnd {
        self.0
    }
}

impl InboundRowType for FileNameRow {
    fn range_end(&self) -> RangeEnd {
        self.1
    }
}

impl InboundRowType for RangeRow {
    fn range_end(&self) -> RangeEnd {
        self.1
    }
}

/// A stream of records returned from the `file_metadata` table in the [`MetadataDb`].
/// ## Assumptions and Guarantees
/// - Sorting: The inner stream is assumed to be ordered by the following fields *_and_*
/// sort order in the `file_metadata` table:
///   1. `nozzle_meta->>'range_start' ASC`
///   2. `nozzle_meta->>'range_end' DESC`
/// - Uniqueness: The inner stream is assumed to be distinct on the following fields:
///   1. `nozzle_meta->>'range_start'`
///   2. `nozzle_meta->>'range_end'`
pub(crate) struct NozzleMetaStream<'a, Inbound: InboundRowType, InboundError> {
    /// The inner stream that is being adapted.
    inner: BoxStream<'a, Result<Inbound, InboundError>>,
    max_block: i64,
}

impl<'a, Inbound: InboundRowType, InboundError> Stream
    for NozzleMetaStream<'a, Inbound, InboundError>
{
    type Item = Result<Inbound, InboundError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner)
            .try_skip_while(Inbound::is_overlapping(this.max_block))
            .poll_next_unpin(cx)
        {
            Pending => Pending,
            Ready(None) => Ready(None),
            Ready(Some(Err(source))) => Ready(Some(Err(source))),
            Ready(Some(Ok(item))) => {
                this.max_block = item.range_end();
                Ready(Some(Ok(item)))
            }
        }
    }
}

pub(crate) trait NozzleMetaStreamExt<
    'a,
    Inbound: InboundRowType + 'a,
    Outbound: 'a,
    InboundError: 'a,
    OutboundError: 'a,
>
{
    fn as_non_overlapping_stream(
        self,
        f: impl FnMut(Result<Inbound, InboundError>) -> Result<Outbound, OutboundError> + Send + 'a,
    ) -> BoxStream<'a, Result<Outbound, OutboundError>>;
}

impl<'a, Inbound: InboundRowType + 'a, Outbound: 'a, InboundError: 'a, OutboundError: 'a>
    NozzleMetaStreamExt<'a, Inbound, Outbound, InboundError, OutboundError>
    for BoxStream<'a, Result<Inbound, InboundError>>
{
    fn as_non_overlapping_stream(
        self,
        f: impl FnMut(Result<Inbound, InboundError>) -> Result<Outbound, OutboundError> + Send + 'a,
    ) -> BoxStream<'a, Result<Outbound, OutboundError>> {
        {
            NozzleMetaStream {
                inner: self,
                max_block: i64::MIN,
            }
            .map(f)
            .boxed()
        }
    }
}

pub(crate) fn map_file_metadata_row(
    row: Result<FileMetaRow, SqlxError>,
) -> Result<(FileName, ObjectMeta, ScannedRangeJson), ObjectStoreError> {
    let (_, file_name, url, last_modified, size, version, e_tag, meta) =
        row.map_err(generic_object_store_error("MetadataDb"))?;
    let url = Url::parse(&url)
        .map(|url| url.join(&file_name))
        .map_err(generic_object_store_error("URL"))?
        .map_err(generic_object_store_error("URL"))?;
    let location = Path::from_url_path(url.path()).map_err(|source| InvalidPath { source })?;

    let size = size as u64;

    let last_modified = last_modified.and_utc();

    let object_meta = ObjectMeta {
        location,
        last_modified,
        size,
        version,
        e_tag,
    };

    Ok((file_name, object_meta, meta))
}

pub(crate) fn map_file_name_row(
    row: Result<FileNameRow, SqlxError>,
) -> Result<FileName, SqlxError> {
    let (file_name, ..) = row?;
    Ok(file_name)
}

pub(crate) fn map_scanned_range_row(
    row: Result<RangeRow, SqlxError>,
) -> Result<(u64, u64), SqlxError> {
    let (range_start, range_end) = row?;
    Ok((range_start as u64, range_end as u64))
}

fn generic_object_store_error<E: Into<Box<dyn Error + Send + Sync + 'static>>>(
    store: &'static str,
) -> impl FnOnce(E) -> ObjectStoreError {
    move |source| Generic {
        store,
        source: source.into(),
    }
}
