use std::{ops::Range, sync::Arc};

use amp_data_store::{CachedParquetData, DataStore, GetCachedMetadataError};
use bytes::Bytes;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::physical_plan::{ParquetFileMetrics, ParquetFileReaderFactory},
    error::{DataFusionError, Result as DataFusionResult},
    parquet::{
        arrow::{
            arrow_reader::ArrowReaderOptions,
            async_reader::{AsyncFileReader, ParquetObjectReader},
        },
        errors::{ParquetError, Result as ParquetResult},
        file::metadata::ParquetMetaData,
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use metadata_db::{files::FileId, physical_table_revision::LocationId};
use tracing::Instrument as _;

/// Factory that creates [`AmpReader`] instances for DataFusion's Parquet scan execution.
#[derive(Debug, Clone)]
pub struct AmpReaderFactory {
    /// The physical table revision location this factory reads from.
    pub location_id: LocationId,
    /// Object store used to read Parquet files.
    pub store: DataStore,
    /// Arrow schema shared across all files read by this factory.
    pub schema: SchemaRef,
}

impl AmpReaderFactory {
    /// Retrieves cached Parquet metadata for the given file, deserializing the footer if needed.
    pub async fn get_cached_metadata(
        &self,
        file: FileId,
    ) -> Result<CachedParquetData, GetCachedMetadataError> {
        self.store
            .get_cached_parquet_metadata(file, self.schema.clone())
            .await
    }
}

impl ParquetFileReaderFactory for AmpReaderFactory {
    /// Creates an [`AmpReader`] for a partitioned file, wiring up metrics and cached metadata.
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DataFusionResult<Box<dyn AsyncFileReader + Send>> {
        let file_meta = &partitioned_file.object_meta;
        let path = &file_meta.location;
        let file_metrics = ParquetFileMetrics::new(partition_index, path.as_ref(), metrics);
        let inner = self
            .store
            .create_file_reader_from_path(path.clone())
            .with_file_size(file_meta.size);
        let location_id = self.location_id;
        let file_id = partitioned_file
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
            store: self.store.clone(),
            schema: self.schema.clone(),
        }))
    }
}

/// Async Parquet file reader that serves byte ranges from the object store and metadata from cache.
pub struct AmpReader {
    /// The physical table revision location being read.
    pub location_id: LocationId,
    /// Identifier of the Parquet file in the metadata database.
    pub file_id: FileId,
    /// DataFusion metrics (bytes scanned, etc.) for this file read.
    pub file_metrics: ParquetFileMetrics,
    /// Underlying object-store reader that fetches byte ranges.
    pub inner: ParquetObjectReader,
    /// Object store used for cached metadata lookups.
    pub store: DataStore,
    /// Arrow schema for deserializing record batches.
    pub schema: SchemaRef,
}

impl AsyncFileReader for AmpReader {
    /// Reads a contiguous byte range from the underlying object store, recording bytes scanned.
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        let span = tracing::info_span!(
            "get_bytes",
            %self.file_id,
            offset = range.start,
            len = bytes_scanned,
        );
        Box::pin(self.inner.get_bytes(range).instrument(span))
    }

    /// Reads multiple byte ranges in a single request, recording total bytes scanned.
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let total_bytes: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total_bytes as usize);
        let span = tracing::info_span!(
            "get_byte_ranges",
            %self.file_id,
            num_ranges = ranges.len(),
            total_bytes = total_bytes,
        );
        Box::pin(self.inner.get_byte_ranges(ranges).instrument(span))
    }

    /// Returns cached Parquet metadata for this file, avoiding a re-read of the footer.
    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let store = self.store.clone();
        let schema = self.schema.clone();
        let file_id = self.file_id;
        let span = tracing::info_span!("get_metadata", %file_id);

        Box::pin(
            async move {
                store
                    .get_cached_parquet_metadata(file_id, schema)
                    .await
                    .map(|cached| cached.metadata)
                    .map_err(|err: amp_data_store::GetCachedMetadataError| {
                        ParquetError::External(err.into())
                    })
            }
            .instrument(span),
        )
    }
}
