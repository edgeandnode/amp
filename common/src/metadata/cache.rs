use std::{ops::Range, sync::Arc};

use dashmap::DashMap;
use datafusion::{
    datasource::physical_plan::{FileMeta, ParquetFileReaderFactory},
    error::{DataFusionError, Result as DataFusionResult},
    execution::cache::CacheAccessor,
    parquet::{
        arrow::{
            arrow_reader::ArrowReaderOptions,
            async_reader::{AsyncFileReader, ParquetObjectReader},
        },
        errors::Result as ParquetResult,
        file::metadata::ParquetMetaData,
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::{
    future::{ok as ok_future, BoxFuture},
    FutureExt,
};
use metadata_db::{FileId, MetadataHash};
use object_store::ObjectStore;

use super::FileMetadata;

#[derive(Debug, Clone)]
pub struct MetadataCache {
    pub metadata: DashMap<FileId, Arc<FileMetadata>>,
    pub store: Arc<dyn ObjectStore>,
}

impl MetadataCache {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            metadata: DashMap::new(),
            store,
        }
    }
}

impl CacheAccessor<FileId, Arc<FileMetadata>> for MetadataCache {
    type Extra = MetadataHash;

    fn clear(&self) {
        self.metadata.clear();
    }

    fn contains_key(&self, k: &FileId) -> bool {
        self.metadata.contains_key(k)
    }

    fn get(&self, _k: &FileId) -> Option<Arc<FileMetadata>> {
        panic!("MetadataCache does not support get without providing the extra metadata hash parameter");
    }

    fn get_with_extra(&self, k: &FileId, e: &Self::Extra) -> Option<Arc<FileMetadata>> {
        self.metadata
            .get(k)
            .filter(|v| v.metadata_hash == *e)
            .map(|v| v.value().clone())
    }

    fn is_empty(&self) -> bool {
        self.metadata.is_empty()
    }

    fn len(&self) -> usize {
        self.metadata.len()
    }

    fn name(&self) -> String {
        "MetadataCache".to_string()
    }

    fn put(&self, key: &FileId, value: Arc<FileMetadata>) -> Option<Arc<FileMetadata>> {
        self.metadata.insert(*key, value)
    }

    fn put_with_extra(
        &self,
        key: &FileId,
        value: Arc<FileMetadata>,
        e: &Self::Extra,
    ) -> Option<Arc<FileMetadata>> {
        if value.metadata_hash == *e {
            self.metadata.insert(*key, value)
        } else {
            panic!(
                "Metadata hash mismatch: expected {:?}, got {:?}",
                e, value.metadata_hash
            );
        }
    }

    fn remove(&mut self, k: &FileId) -> Option<Arc<FileMetadata>> {
        self.metadata.remove(k).map(|(_, v)| v.clone())
    }
}

impl ParquetFileReaderFactory for MetadataCache {
    fn create_reader(
        &self,
        _partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DataFusionResult<Box<dyn AsyncFileReader + Send>> {
        let path = file_meta.object_meta.location.clone();
        let mut inner = ParquetObjectReader::new(self.store.clone(), path)
            .with_file_size(file_meta.object_meta.size);
        if let Some(size_hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(size_hint);
        }

        let (file_id, metadata_hash) = file_meta
            .extensions
            .ok_or(DataFusionError::Execution(format!(
                "FileMeta for {} is missing extensions",
                file_meta.object_meta.location
            )))?
            .downcast::<(FileId, MetadataHash)>()
            .map(|extensions| (extensions.0, extensions.1))
            .map_err(|_| {
                DataFusionError::Execution(format!(
                    "FileMeta extensions for {} are not of type (FileId, MetadataHash)",
                    file_meta.object_meta.location
                ))
            })?;

        let metadata = self
            .get_with_extra(&file_id, &metadata_hash)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Metadata for file id {} with hash {:?} not found in cache for file at location {}",
                    file_id, metadata_hash, file_meta.object_meta.location
                ))
            })?
            .metadata
            .clone();

        let reader = NozzleReader { metadata, inner };

        Ok(Box::new(reader))
    }
}

#[derive(Debug, Clone)]
pub struct NozzleReader {
    pub metadata: Arc<ParquetMetaData>,
    pub inner: ParquetObjectReader,
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
