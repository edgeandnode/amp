use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use dashmap::DashMap;
use datafusion::{
    datasource::physical_plan::ParquetFileReaderFactory,
    error::DataFusionError,
    parquet::{
        arrow::{
            arrow_reader::ArrowReaderOptions,
            async_reader::{AsyncFileReader, ParquetObjectReader},
        },
        errors::Result as ParquetResult,
        file::metadata::{ParquetMetaData, ParquetMetaDataReader},
    },
};
use futures::{
    future::{ok as ok_future, BoxFuture},
    FutureExt,
};
use object_store::ObjectStore;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Debug, Clone)]
pub(super) struct NozzleParquetFileReaderFactory {
    pub(super) metadata_generator: Arc<DashMap<usize, mpsc::Receiver<Vec<u8>>>>,
    pub(super) object_store: Arc<dyn ObjectStore>,
}

impl NozzleParquetFileReaderFactory {
    fn next(&self, group_id: usize) -> Option<Arc<ParquetMetaData>> {
        if let Some(mut receiver) = self.metadata_generator.get_mut(&group_id) {
            if let Some(metadata) = receiver.blocking_recv() {
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
    #[tracing::instrument(skip_all)]
    fn create_reader(
        &self,
        _partition_index: usize,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
        metadata_size_hint: Option<usize>,
        _metrics: &datafusion::physical_plan::metrics::ExecutionPlanMetricsSet,
    ) -> Result<
        Box<dyn datafusion::parquet::arrow::async_reader::AsyncFileReader + Send>,
        DataFusionError,
    > {
        warn!(
            "NozzleParquetFileReaderFactory::create_reader called with file_meta: {:?}",
            file_meta.location()
        );
        let file_group_id: usize = *file_meta
            .extensions
            .ok_or(DataFusionError::Plan(
                "FileMeta must contain extensions with group_id".to_string(),
            ))?
            .downcast()
            .map_err(|e| {
                let e_type = e.type_id();
                DataFusionError::Internal(format!(
                    "Failed to downcast extensions to usize, type_id: {:?}",
                    e_type
                ))
            })?;
        let metadata = self
            .next(file_group_id)
            .ok_or(DataFusionError::Execution(format!(
                "No metadata found for group_id: {}",
                file_group_id
            )))?;

        let path = file_meta.object_meta.location.clone();
        let mut inner = ParquetObjectReader::new(self.object_store.clone(), path);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint);
        }
        let reader = NozzleReader { metadata, inner };
        Ok(Box::new(reader))
    }
}

#[derive(Debug, Clone)]
pub struct NozzleReader {
    metadata: Arc<ParquetMetaData>,
    inner: ParquetObjectReader,
}

impl AsyncFileReader for NozzleReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        ok_future(self.metadata.clone()).boxed()
    }
}
