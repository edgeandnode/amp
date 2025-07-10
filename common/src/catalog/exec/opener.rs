use std::sync::Arc;

use datafusion::{
    arrow::{
        datatypes::{SchemaRef, TimeUnit},
        error::ArrowError,
    },
    datasource::{
        file_format::parquet::{apply_file_schema_type_coercions, coerce_int96_to_resolution},
        physical_plan::{
            FileMeta, FileOpenFuture, FileOpener, ParquetFileMetrics,
            parquet::{ParquetAccessPlan, RowGroupAccessPlanFilter, build_row_filter},
        },
        schema_adapter::SchemaAdapterFactory,
    },
    error::Result as DataFusionResult,
    parquet::arrow::{
        ParquetRecordBatchStreamBuilder, ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::AsyncFileReader,
    },
    physical_plan::{
        PhysicalExpr,
        metrics::{ExecutionPlanMetricsSet, MetricBuilder},
    },
};
use futures::{StreamExt, TryStreamExt};
use metadata_db::MetadataDb;
use object_store::ObjectStore;

use crate::catalog::exec::{reader::NozzleReader, source::NozzleSource};

pub(super) struct NozzleOpener {
    pub(super) metadata_db: Arc<MetadataDb>,
    pub(super) object_store: Arc<dyn ObjectStore>,

    // Execution plan related members
    pub(super) limit: Option<usize>,
    pub(super) predicate: Arc<dyn PhysicalExpr>,
    pub(super) projection: Arc<[usize]>,

    // Schema and type coercion related members
    pub(super) coerce_int96: Option<TimeUnit>,
    pub(super) file_schema: SchemaRef,
    pub(super) schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,

    // Metrics related members
    pub(super) partition_index: usize,
    pub(super) metrics: ExecutionPlanMetricsSet,

    // Parquet options related members
    pub(super) batch_size: usize,
    pub(super) enable_bloom_filter: bool,
    pub(super) pushdown_filters: bool,
    pub(super) enable_page_index: bool,
    pub(super) enable_row_group_stats_pruning: bool,
    pub(super) reorder_filters: bool,
}

impl FileOpener for NozzleOpener {
    fn open(&self, file_meta: FileMeta) -> DataFusionResult<FileOpenFuture> {
        let filename = file_meta.location().as_ref();
        let partition = self.partition_index;

        let metrics = self.metrics.clone();
        let file_metrics = ParquetFileMetrics::new(partition, filename, &metrics);

        let predicate_creation_errors =
            MetricBuilder::new(&self.metrics).global_counter("num_predicate_creation_errors");

        let projected_schema = SchemaRef::from(self.file_schema.project(&self.projection)?);

        let schema_adapter_factory = Arc::clone(&self.schema_adapter_factory);

        let schema_adapter = self
            .schema_adapter_factory
            .create(projected_schema, Arc::clone(&self.file_schema));

        let coerce_int96 = self.coerce_int96;

        let logical_file_schema = Arc::clone(&self.file_schema);

        let reorder_predicates = self.reorder_filters;
        let predicate = Arc::clone(&self.predicate.clone());
        let limit = self.limit;

        let pushdown_filters = self.pushdown_filters;
        let enable_bloom_filter = self.enable_bloom_filter;
        let enable_page_index = self.enable_page_index;
        let enable_row_group_stats_pruning = self.enable_row_group_stats_pruning;
        let batch_size = self.batch_size;

        let file_range = file_meta.range.clone();

        let metadata_db = Arc::clone(&self.metadata_db);
        let object_store = Arc::clone(&self.object_store);

        Ok(Box::pin(async move {
            let mut options = ArrowReaderOptions::new().with_page_index(true);
            let mut async_file_reader = NozzleReader::try_new_boxed(
                metadata_db,
                object_store,
                partition,
                file_meta,
                &metrics,
            )
            .await?;
            let mut metadata_timer = file_metrics.metadata_load_time.timer();

            let metadata = async_file_reader.get_metadata(None).await?;
            let mut reader_metadata = ArrowReaderMetadata::try_new(metadata, options.clone())?;

            let mut physical_file_schema = Arc::clone(reader_metadata.schema());

            if let Some(merged) =
                apply_file_schema_type_coercions(&logical_file_schema, &physical_file_schema)
            {
                physical_file_schema = Arc::new(merged);
                options = options.with_schema(Arc::clone(&physical_file_schema));
                reader_metadata = ArrowReaderMetadata::try_new(
                    Arc::clone(reader_metadata.metadata()),
                    options.clone(),
                )?;
            }

            if coerce_int96.is_some() {
                if let Some(merged) = coerce_int96_to_resolution(
                    reader_metadata.parquet_schema(),
                    &physical_file_schema,
                    &(coerce_int96.unwrap()),
                ) {
                    physical_file_schema = Arc::new(merged);
                    options = options.with_schema(Arc::clone(&physical_file_schema));
                    reader_metadata = ArrowReaderMetadata::try_new(
                        Arc::clone(reader_metadata.metadata()),
                        options.clone(),
                    )?;
                }
            }

            let (pruning_predicate, page_pruning_predicate) =
                NozzleSource::build_pruning_predicates(
                    &predicate,
                    &logical_file_schema,
                    &predicate_creation_errors,
                );

            metadata_timer.stop();

            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                async_file_reader,
                reader_metadata,
            );

            let (schema_mapping, adapted_projections) =
                schema_adapter.map_schema(&physical_file_schema)?;
            let indices = adapted_projections.into_iter();

            let mask = ProjectionMask::roots(builder.parquet_schema(), indices);

            // Filter pushdown: evaluate predicates during scan
            if let Some(expr) = pushdown_filters.then_some(predicate) {
                let row_filter = build_row_filter(
                    &expr,
                    &physical_file_schema,
                    &logical_file_schema,
                    builder.metadata(),
                    reorder_predicates,
                    &file_metrics,
                    &schema_adapter_factory,
                );

                match row_filter {
                    Ok(Some(filter)) => {
                        builder = builder.with_row_filter(filter);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::debug!("Ignoring error building row filter: {e}");
                    }
                };
            };

            let file_metadata = Arc::clone(builder.metadata());
            let predicate = pruning_predicate.as_ref().map(|p| p.as_ref());
            let rg_metadata = file_metadata.row_groups();

            let access_plan = ParquetAccessPlan::new_all(rg_metadata.len());
            let mut row_groups = RowGroupAccessPlanFilter::new(access_plan);

            if let Some(range) = file_range.as_ref() {
                row_groups.prune_by_range(rg_metadata, range);
            }

            if let Some(predicate) = predicate.as_ref() {
                if enable_row_group_stats_pruning {
                    row_groups.prune_by_statistics(
                        &physical_file_schema,
                        builder.parquet_schema(),
                        rg_metadata,
                        predicate,
                        &file_metrics,
                    );
                }

                if enable_bloom_filter && !row_groups.is_empty() {
                    row_groups
                        .prune_by_bloom_filters(
                            &physical_file_schema,
                            &mut builder,
                            predicate,
                            &file_metrics,
                        )
                        .await;
                }
            }

            let mut access_plan = row_groups.build();

            if enable_page_index && !access_plan.is_empty() {
                access_plan = page_pruning_predicate.prune_plan_with_page_index(
                    access_plan,
                    &physical_file_schema,
                    builder.parquet_schema(),
                    file_metadata.as_ref(),
                    &file_metrics,
                );
            }

            let row_group_indexes = access_plan.row_group_indexes();
            if let Some(row_selection) = access_plan.into_overall_row_selection(rg_metadata)? {
                builder = builder.with_row_selection(row_selection);
            }

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_group_indexes)
                .build()?;

            let adapted = stream
                .map_err(|e| ArrowError::ParquetError(e.to_string()))
                .map(move |maybe_batch| {
                    maybe_batch.and_then(|b| schema_mapping.map_batch(b).map_err(ArrowError::from))
                });

            Ok(adapted.boxed())
        }))
    }
}
