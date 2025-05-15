use std::{
    pin::Pin,
    task::Poll::{self, Pending, Ready},
};

use futures::{
    future,
    stream::{BoxStream, StreamExt, TryStreamExt},
    Stream,
};
use object_store::{
    path::Path,
    Error::{self as ObjectStoreError, Generic, InvalidPath},
    ObjectMeta,
};
use serde_json::Value as NozzleMetaJson;
use sqlx::{types::chrono::NaiveDateTime, Error as SqlxError};
use url::{ParseError as UrlParseError, Url};

type FileMetaItem = Result<(String, ObjectMeta, NozzleMetaJson), ObjectStoreError>;

pub(crate) type FileMetaRow = (
    i64,
    String,
    String,
    NaiveDateTime,
    i64,
    Option<String>,
    Option<String>,
    NozzleMetaJson,
);

/// A stream of records returned from the `file_metadata` table in the [`MetadataDb`].
/// ## Assumptions and Guarantees
/// - Sorting: The inner stream is assumed to be ordered by the following fields *_and_*
/// sort order in the `file_metadata` table:
///   1. `nozzle_meta->>'range_start' ASC`
///   2. `nozzle_meta->>'range_end' DESC`
/// - Uniqueness: The inner stream is assumed to be distinct on the following fields:
///   1. `nozzle_meta->>'range_start'`
///   2. `nozzle_meta->>'range_end'`
struct NozzleMetaStream<'a, T> {
    /// The inner stream that is being adapted.
    inner: BoxStream<'a, Result<T, SqlxError>>,
    max_block: i64,
}

fn generic_url_error(source: UrlParseError) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "URL",
        source: source.into(),
    }
}

impl<'a> Stream for NozzleMetaStream<'a, FileMetaRow> {
    type Item = FileMetaItem;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner)
            .try_skip_while(|(range_end, ..)| future::ok(*range_end <= this.max_block))
            .poll_next_unpin(cx)
        {
            Pending => Pending,
            Ready(None) => Ready(None),
            Ready(Some(Err(source))) => Ready(Some(Err(Generic {
                store: "MetadataDb",
                source: source.into(),
            }))),
            Ready(Some(Ok((
                range_end,
                file_name,
                url,
                last_modified,
                size,
                e_tag,
                version,
                meta,
            )))) => {
                let url = Url::parse(&url)
                    .map(|url| url.join(&file_name))
                    .map_err(generic_url_error)?
                    .map_err(generic_url_error)?;

                let location =
                    Path::from_url_path(url.path()).map_err(|source| InvalidPath { source })?;

                let size = size as u64;

                let last_modified = last_modified.and_utc();

                let object_meta = ObjectMeta {
                    location,
                    last_modified,
                    size,
                    version,
                    e_tag,
                };
                this.max_block = range_end;

                Ready(Some(Ok((file_name, object_meta, meta))))
            }
        }
    }
}

impl<'a> From<BoxStream<'a, Result<FileMetaRow, SqlxError>>> for NozzleMetaStream<'a, FileMetaRow> {
    fn from(value: BoxStream<'a, Result<FileMetaRow, SqlxError>>) -> Self {
        Self {
            inner: value,
            max_block: i64::MIN,
        }
    }
}

impl<'a> Stream for NozzleMetaStream<'a, (i64, i64)> {
    type Item = Result<(u64, u64), SqlxError>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner)
            .try_skip_while(|(_, range_end)| future::ok(*range_end <= this.max_block))
            .poll_next_unpin(cx)
        {
            Pending => Pending,
            Ready(None) => Ready(None),
            Ready(Some(Err(source))) => Ready(Some(Err(source))),
            Ready(Some(Ok((range_start, range_end)))) => {
                this.max_block = range_end;
                Ready(Some(Ok((range_start as u64, range_end as u64))))
            }
        }
    }
}

impl<'a> From<BoxStream<'a, Result<(i64, i64), SqlxError>>> for NozzleMetaStream<'a, (i64, i64)> {
    fn from(value: BoxStream<'a, Result<(i64, i64), SqlxError>>) -> Self {
        Self {
            inner: value,
            max_block: i64::MIN,
        }
    }
}

impl<'a> Stream for NozzleMetaStream<'a, (String, i64)> {
    type Item = Result<String, SqlxError>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner)
            .try_skip_while(|(_, range_end)| future::ok(*range_end <= this.max_block))
            .poll_next_unpin(cx)
        {
            Pending => Pending,
            Ready(None) => Ready(None),
            Ready(Some(Err(source))) => Ready(Some(Err(source))),
            Ready(Some(Ok((file_name, range_end)))) => {
                this.max_block = range_end;
                Ready(Some(Ok(file_name)))
            }
        }
    }
}

impl<'a> From<BoxStream<'a, Result<(String, i64), SqlxError>>>
    for NozzleMetaStream<'a, (String, i64)>
{
    fn from(value: BoxStream<'a, Result<(String, i64), SqlxError>>) -> Self {
        Self {
            inner: value,
            max_block: i64::MIN,
        }
    }
}

pub trait NozzleMetaStreamExt<'a, T: 'a> {
    fn as_non_overlapping_stream(self) -> BoxStream<'a, T>;
}

impl<'a, T: 'a, U: 'a, S: Stream<Item = Result<T, SqlxError>> + 'a> NozzleMetaStreamExt<'a, U> for S
where
    NozzleMetaStream<'a, T>: From<S> + Stream<Item = U> + 'a,
{
    /// Adapts the stream to filter out all files whose range overlaps some other,
    /// larger range.
    ///
    /// See [`NozzleMetaStream`] for more details.
    fn as_non_overlapping_stream(self) -> BoxStream<'a, U> {
        NozzleMetaStream::from(self).boxed()
    }
}
