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
use metadata_db::{FileId, LocationId, MetadataDb};
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
) -> Result<CachedParquetData, foyer::Error> {
    let file_id = file;

    cache
        .fetch(file_id, || async move {
            // Cache miss, fetch from database
            let footer = metadata_db
                .get_file_footer_bytes(file_id)
                .await
                .map_err(|e| foyer::Error::Other(e.into()))?;

            let metadata = Arc::new(
                ParquetMetaDataReader::new()
                    .with_page_index_policy(PageIndexPolicy::Required)
                    .parse_and_finish(&Bytes::from_owner(footer))
                    .map_err(|e| foyer::Error::Other(e.into()))?,
            );

            let statistics = Arc::new(
                DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &schema)
                    .map_err(|e| foyer::Error::Other(e.into()))?,
            );

            Ok(CachedParquetData {
                metadata,
                statistics,
            })
        })
        .await
        .map(|c| c.value().clone())
}
