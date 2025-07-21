use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use datafusion::{
    datasource::physical_plan::{FileMeta, ParquetFileMetrics, ParquetFileReaderFactory},
    error::Result as DataFusionResult,
    parquet::{
        arrow::{
            arrow_reader::ArrowReaderOptions,
            async_reader::{AsyncFileReader, ParquetObjectReader},
        },
        errors::{ParquetError, Result as ParquetResult},
        file::metadata::{ParquetMetaData, ParquetMetaDataReader},
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::future::BoxFuture;
use metadata_db::{LocationId, MetadataDb};
use object_store::ObjectStore;

#[derive(Debug, Clone)]
pub struct NozzleReaderFactory {
    pub location_id: LocationId,
    pub metadata_db: Arc<MetadataDb>,
    pub object_store: Arc<dyn ObjectStore>,
}

impl ParquetFileReaderFactory for NozzleReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DataFusionResult<Box<dyn AsyncFileReader + Send>> {
        let path = file_meta.location();
        let file_metrics = ParquetFileMetrics::new(partition_index, path.as_ref(), metrics);
        let metadata_db = Arc::clone(&self.metadata_db);
        let store = Arc::clone(&self.object_store);
        let inner = ParquetObjectReader::new(store, path.clone())
            .with_file_size(file_meta.object_meta.size);
        let location_id = self.location_id;

        Ok(Box::new(NozzleReader {
            location_id,
            inner,
            file_metrics,
            metadata_db,
            file_meta,
        }))
    }
}

pub struct NozzleReader {
    pub location_id: LocationId,
    pub file_meta: FileMeta,
    pub metadata_db: Arc<MetadataDb>,
    pub file_metrics: ParquetFileMetrics,
    pub inner: ParquetObjectReader,
}

impl AsyncFileReader for NozzleReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let total_bytes: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total_bytes as usize);
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let metadata_db = Arc::clone(&self.metadata_db);
        let file_meta = &self.file_meta;
        let location_id = self.location_id;
        Box::pin(async move {
            let file_name = file_meta
                .location()
                .filename()
                .ok_or(ParquetError::External("File name is not available".into()))?
                .to_string();
            let footer = metadata_db
                .get_footer_bytes(location_id, file_name)
                .await
                .map_err(|e| ParquetError::External(e.into()))?;

            let metadata = ParquetMetaDataReader::new()
                .with_page_indexes(true)
                .parse_and_finish(&Bytes::from_owner(footer))?;
            Ok(Arc::new(metadata))
        })
    }
}
