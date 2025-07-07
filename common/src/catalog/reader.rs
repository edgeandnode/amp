#![allow(unreachable_code, unused_variables)]
use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use dashmap::DashMap;
use datafusion::{
    datasource::physical_plan::{FileMeta, ParquetFileMetrics, ParquetFileReaderFactory},
    error::DataFusionError,
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
    future::{ok as ok_future, BoxFuture},
    FutureExt,
};
use object_store::ObjectStore;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub(super) struct NozzleParquetFileReaderFactory {
    pub(super) metadata_generator: Arc<DashMap<usize, mpsc::Receiver<Vec<u8>>>>,
    pub(super) object_store: Arc<dyn ObjectStore>,
}

impl NozzleParquetFileReaderFactory {
    fn next(&self, group_id: usize) -> Option<Arc<ParquetMetaData>> {
        if let Some(mut receiver) = self.metadata_generator.get_mut(&group_id) {
            if let Some(metadata) = tokio::task::block_in_place(move || receiver.blocking_recv()) {
                let metadata: Arc<ParquetMetaData> = ParquetMetaDataReader::new()
                    .with_page_indexes(true)
                    .parse_and_finish(&Bytes::from_iter(metadata))
                    .ok()?
                    .into();
                return Some(metadata);
            } else {
                return None; // No more metadata available for this group
            }
        } else {
            return None; // Group ID not found
        }
    }
}

impl ParquetFileReaderFactory for NozzleParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>, DataFusionError> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file_meta.location().as_ref(), metrics);

        let group_id = *file_meta
            .extensions
            .ok_or(DataFusionError::Plan(
                "FileMeta must contain extensions with group_id".to_string(),
            ))?
            .downcast::<usize>()
            .map_err(|e| {
                let e_type = e.type_id();
                DataFusionError::Internal(format!(
                    "Failed to downcast extensions to usize, type_id: {:?}",
                    e_type
                ))
            })?;

        let metadata = self
            .next(group_id)
            .ok_or(DataFusionError::Execution(format!(
                "No metadata found for group_id: {}",
                group_id
            )))?;

        let path = file_meta.object_meta.location.clone();

        let mut inner = ParquetObjectReader::new(self.object_store.clone(), path)
            .with_file_size(file_meta.object_meta.size);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint);
        }

        let reader = NozzleReader {
            metadata,
            file_metrics,
            inner,
        };

        Ok(Box::new(reader))
    }
}

#[derive(Debug, Clone)]
pub struct NozzleReader {
    metadata: Arc<ParquetMetaData>,
    file_metrics: ParquetFileMetrics,
    inner: ParquetObjectReader,
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
