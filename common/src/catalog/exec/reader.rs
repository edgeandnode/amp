use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use datafusion::{
    datasource::physical_plan::{FileMeta, ParquetFileMetrics},
    error::{DataFusionError, Result as DataFusionResult},
    parquet::{
        arrow::{
            arrow_reader::ArrowReaderOptions,
            async_reader::{AsyncFileReader, ParquetObjectReader},
        },
        errors::Result as ParquetResult,
        file::metadata::{ParquetMetaData, ParquetMetaDataReader},
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::{
    TryFutureExt,
    future::{BoxFuture, FutureExt, ok as ok_future},
};
use metadata_db::{FileId, MetadataDb};
use object_store::ObjectStore;

#[derive(Debug, Clone)]
pub(super) struct NozzleReader {
    metadata: Arc<ParquetMetaData>,
    file_metrics: ParquetFileMetrics,
    inner: ParquetObjectReader,
}

impl NozzleReader {
    pub(super) fn try_new_boxed<'a>(
        metadata_db: Arc<MetadataDb>,
        object_store: Arc<dyn ObjectStore>,
        partition: usize,
        file_meta: FileMeta,
        metrics: &'a ExecutionPlanMetricsSet,
    ) -> impl Future<Output = DataFusionResult<Box<dyn AsyncFileReader>>> + 'a {
        Self::try_new(file_meta, metadata_db, object_store, partition, metrics)
            .map_ok(|reader| Box::new(reader) as Box<dyn AsyncFileReader>)
    }

    async fn try_new(
        file_meta: FileMeta,
        metadata_db: Arc<MetadataDb>,
        object_store: Arc<dyn ObjectStore>,
        partition: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DataFusionResult<Self> {
        let path = file_meta.location().clone();
        let file_id = file_meta
            .extensions
            .ok_or(DataFusionError::Execution(format!(
                "FileMeta missing extensions for : {path}",
            )))?
            .downcast::<FileId>()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "FileMeta extensions for {path} are not of type FileId. Found: {:?} instead.",
                    e.as_ref()
                ))
            })?;

        let reader = metadata_db
            .get_footer_bytes(&file_id)
            .await
            .map_err(|e| DataFusionError::External(e.into()))
            .map(|owner| Bytes::from_owner(owner))?;

        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_and_finish(&reader)?
            .into();

        let file_metrics = ParquetFileMetrics::new(partition, path.as_ref(), metrics);

        let inner =
            ParquetObjectReader::new(object_store, path).with_file_size(file_meta.object_meta.size);

        Ok(Self {
            metadata,
            file_metrics,
            inner,
        })
    }
}

impl AsyncFileReader for NozzleReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let total = ranges.iter().map(|r| r.end - r.start).sum::<u64>() as usize;
        self.file_metrics.bytes_scanned.add(total);
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        ok_future(self.metadata.clone()).boxed()
    }
}
