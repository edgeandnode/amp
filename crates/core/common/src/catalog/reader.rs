use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::Statistics,
    datasource::physical_plan::{
        FileMeta, ParquetFileMetrics, ParquetFileReaderFactory,
        parquet::metadata::DFParquetMetadata,
    },
    error::{DataFusionError, Result as DataFusionResult},
    parquet::{
        arrow::{
            arrow_reader::ArrowReaderOptions,
            async_reader::{AsyncFileReader, ParquetObjectReader},
        },
        errors::{ParquetError, Result as ParquetResult},
        file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use foyer::Cache;
use futures::{TryFutureExt as _, future::BoxFuture};
use metadata_db::{LocationId, MetadataDb, files::FileId};
use object_store::ObjectStore;

use crate::BoxError;

/// Cached parquet data including metadata and computed statistics.
#[derive(Clone)]
pub struct CachedParquetData {
    pub metadata: Arc<ParquetMetaData>,
    pub statistics: Arc<Statistics>,
}

type ParquetMetaDataCache = Cache<FileId, CachedParquetData>;

#[derive(Debug, Clone)]
pub struct AmpReaderFactory {
    pub location_id: LocationId,
    pub metadata_db: MetadataDb,
    pub object_store: Arc<dyn ObjectStore>,
    pub parquet_footer_cache: ParquetMetaDataCache,
    pub schema: SchemaRef,
}

impl AmpReaderFactory {
    pub async fn get_cached_metadata(&self, file: FileId) -> Result<CachedParquetData, BoxError> {
        get_cached_metadata(
            file,
            self.parquet_footer_cache.clone(),
            self.metadata_db.clone(),
            self.schema.clone(),
        )
        .await
        .map_err(|e| e.into())
    }
}

impl ParquetFileReaderFactory for AmpReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DataFusionResult<Box<dyn AsyncFileReader + Send>> {
        let path = file_meta.location();
        let file_metrics = ParquetFileMetrics::new(partition_index, path.as_ref(), metrics);
        let metadata_db = self.metadata_db.clone();
        let store = Arc::clone(&self.object_store);
        let inner = ParquetObjectReader::new(store, path.clone())
            .with_file_size(file_meta.object_meta.size);
        let location_id = self.location_id;
        let file_id = file_meta
            .extensions
            .ok_or(DataFusionError::Execution(format!(
                "FileMeta missing extensions for location_id: {}",
                location_id
            )))?
            .downcast::<FileId>()
            .map_err(|_| {
                DataFusionError::Execution("FileMeta extensions are not of type FileId".to_string())
            })?;

        Ok(Box::new(AmpReader {
            location_id,
            file_id: *file_id,
            inner,
            file_metrics,
            metadata_db,
            parquet_footer_cache: self.parquet_footer_cache.clone(),
            schema: self.schema.clone(),
        }))
    }
}

pub struct AmpReader {
    pub location_id: LocationId,
    pub file_id: FileId,
    pub metadata_db: MetadataDb,
    pub file_metrics: ParquetFileMetrics,
    pub inner: ParquetObjectReader,
    pub parquet_footer_cache: ParquetMetaDataCache,
    pub schema: SchemaRef,
}

impl AsyncFileReader for AmpReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let total_bytes: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total_bytes as usize);
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let metadata_db = self.metadata_db.clone();
        let cache = self.parquet_footer_cache.clone();
        let schema = self.schema.clone();

        Box::pin(
            get_cached_metadata(self.file_id, cache, metadata_db, schema)
                .map_ok(|cached| cached.metadata)
                .map_err(|e| ParquetError::External(e.into())),
        )
    }
}

async fn get_cached_metadata(
    file: FileId,
    cache: ParquetMetaDataCache,
    metadata_db: MetadataDb,
    schema: SchemaRef,
) -> Result<CachedParquetData, GetCachedMetadataError> {
    let file_id = file;

    cache
        .get_or_fetch(&file_id, || async move {
            // Cache miss, fetch from database
            let footer = metadata_db::files::get_footer_bytes(&metadata_db, file_id)
                .await
                .map_err(GetCachedMetadataError::FetchFooter)?;

            let metadata = Arc::new(
                ParquetMetaDataReader::new()
                    .with_page_index_policy(PageIndexPolicy::Required)
                    .parse_and_finish(&Bytes::from_owner(footer))
                    .map_err(GetCachedMetadataError::ParseMetadata)?,
            );

            let statistics = Arc::new(
                DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &schema)
                    .map_err(GetCachedMetadataError::ComputeStatistics)?,
            );

            Ok::<CachedParquetData, GetCachedMetadataError>(CachedParquetData {
                metadata,
                statistics,
            })
        })
        .await
        .map(|entry| entry.value().clone())
        .map_err(GetCachedMetadataError::CacheError)
}

/// Errors that occur when fetching cached parquet metadata
#[derive(Debug, thiserror::Error)]
pub enum GetCachedMetadataError {
    /// Failed to fetch file footer bytes from metadata database
    ///
    /// This occurs when the database query to retrieve the parquet file footer fails.
    /// The footer contains critical metadata including schema, row group information,
    /// and page index data required for query planning and execution.
    ///
    /// Possible causes:
    /// - Database connection lost or network interruption
    /// - File ID not found in the metadata database
    /// - Database query timeout
    /// - Insufficient database permissions
    /// - Database server error or unavailability
    #[error("failed to fetch file footer from metadata database")]
    FetchFooter(#[source] metadata_db::Error),

    /// Failed to parse parquet metadata from footer bytes
    ///
    /// This occurs when the parquet library cannot parse the footer bytes into
    /// valid ParquetMetaData. This typically indicates corrupted or invalid parquet
    /// file data.
    ///
    /// Possible causes:
    /// - Corrupted parquet file footer data in the database
    /// - Incompatible parquet format version
    /// - Truncated or incomplete footer bytes
    /// - File was not a valid parquet file
    /// - Parquet file written with incompatible compression or encoding
    ///
    /// This error is not recoverable through retry as it indicates data corruption.
    #[error("failed to parse parquet metadata")]
    ParseMetadata(#[source] ParquetError),

    /// Failed to compute statistics from parquet metadata
    ///
    /// This occurs when DataFusion cannot extract statistics (min/max values,
    /// null counts, etc.) from the parquet metadata. These statistics are used
    /// for query optimization and predicate pushdown.
    ///
    /// Possible causes:
    /// - Schema mismatch between parquet file and expected schema
    /// - Invalid or missing statistics in the parquet metadata
    /// - Incompatible data types between parquet schema and arrow schema
    /// - Corrupted row group metadata
    /// - Unsupported parquet logical types
    ///
    /// This error typically indicates a schema evolution issue or incompatible
    /// parquet file format.
    #[error("failed to compute statistics from parquet metadata")]
    ComputeStatistics(#[source] DataFusionError),

    /// Cache layer error during get or fetch operation
    ///
    /// This occurs when the foyer cache fails during retrieval or storage operations.
    /// The cache provides both in-memory and disk-based caching of parquet metadata
    /// to avoid repeated database queries and parsing overhead.
    ///
    /// Possible causes:
    /// - Disk cache I/O errors (disk full, permissions, hardware failure)
    /// - Memory allocation failure in the in-memory cache
    /// - Cache corruption or checksum mismatch
    /// - Concurrent cache operations conflict
    /// - Cache storage backend unavailable
    ///
    /// This error may be transient and safe to retry. If it persists, it indicates
    /// a problem with the cache storage layer rather than the underlying data.
    #[error("cache layer error")]
    CacheError(#[source] foyer::Error),
}
