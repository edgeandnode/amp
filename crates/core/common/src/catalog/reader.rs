use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::physical_plan::{FileMeta, ParquetFileMetrics, ParquetFileReaderFactory},
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
use futures::future::BoxFuture;
use metadata_db::{LocationId, files::FileId};

use crate::{
    BoxError,
    store::{CachedParquetData, CachedStore},
};

#[derive(Debug, Clone)]
pub struct AmpReaderFactory {
    pub location_id: LocationId,
    pub store: CachedStore,
    pub schema: SchemaRef,
}

impl AmpReaderFactory {
    pub async fn get_cached_metadata(&self, file: FileId) -> Result<CachedParquetData, BoxError> {
        self.store
            .get_cached_parquet_metadata(file, self.schema.clone())
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
        let inner = self
            .store
            .create_revision_file_reader(path.clone())
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
            store: self.store.clone(),
            schema: self.schema.clone(),
        }))
    }
}

pub struct AmpReader {
    pub location_id: LocationId,
    pub file_id: FileId,
    pub file_metrics: ParquetFileMetrics,
    pub inner: ParquetObjectReader,
    pub store: CachedStore,
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
        let store = self.store.clone();
        let schema = self.schema.clone();
        let file_id = self.file_id;

        Box::pin(async move {
            store
                .get_cached_parquet_metadata(file_id, schema)
                .await
                .map(|cached| cached.metadata)
                .map_err(|err| ParquetError::External(err.into()))
        })
    }
}
