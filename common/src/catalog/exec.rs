use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{SchemaRef, TimeUnit},
    datasource::{
        physical_plan::{FileMeta, ParquetFileMetrics},
        schema_adapter::SchemaAdapterFactory,
    },
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{PhysicalExpr, metrics::ExecutionPlanMetricsSet},
};
use metadata_db::MetadataDb;
use object_store::ObjectStore;
use opener::NozzleOpener;

pub(super) mod opener {

    use datafusion::{
        arrow::error::ArrowError,
        datasource::{
            file_format::parquet::{apply_file_schema_type_coercions, coerce_int96_to_resolution},
            physical_plan::{
                FileOpenFuture, FileOpener,
                parquet::{ParquetAccessPlan, RowGroupAccessPlanFilter, build_row_filter},
            },
        },
        parquet::arrow::{
            ParquetRecordBatchStreamBuilder, ProjectionMask,
            arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
            async_reader::AsyncFileReader,
        },
        physical_plan::metrics::MetricBuilder,
    };
    use futures::{StreamExt, TryStreamExt};

    use super::*;
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
                        maybe_batch
                            .and_then(|b| schema_mapping.map_batch(b).map_err(ArrowError::from))
                    });

                Ok(adapted.boxed())
            }))
        }
    }
}

pub(super) mod reader {
    use std::ops::Range;

    use bytes::Bytes;
    use datafusion::parquet::{
        arrow::{
            arrow_reader::ArrowReaderOptions,
            async_reader::{AsyncFileReader, ParquetObjectReader},
        },
        errors::Result as ParquetResult,
        file::metadata::{ParquetMetaData, ParquetMetaDataReader},
    };
    use futures::{
        TryFutureExt,
        future::{BoxFuture, FutureExt, ok as ok_future},
    };
    use metadata_db::FileId;

    use super::*;

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

            let inner = ParquetObjectReader::new(object_store, path)
                .with_file_size(file_meta.object_meta.size);

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
}

pub(super) mod source {
    use std::{
        collections::HashSet,
        fmt::{Formatter, Result as FmtResult},
        ops::Not,
        sync::Arc,
    };

    use datafusion::{
        catalog::Session,
        common::Statistics,
        config::{ConfigOptions, TableParquetOptions},
        datasource::{
            physical_plan::{
                FileOpener, FileScanConfig, FileSource,
                parquet::{PagePruningAccessPlanFilter, can_expr_be_pushed_down_with_schemas},
            },
            schema_adapter::DefaultSchemaAdapterFactory,
        },
        physical_expr::conjunction,
        physical_expr_common::physical_expr::fmt_sql,
        physical_optimizer::pruning::PruningPredicate,
        physical_plan::{
            DisplayFormatType,
            expressions::lit,
            filter_pushdown::{FilterPushdownPropagation, PredicateSupport, PredicateSupports},
            metrics::Count,
        },
    };

    use super::*;

    #[derive(Debug, Clone)]
    pub(in crate::catalog) struct NozzleSource {
        table_parquet_options: TableParquetOptions,
        metrics: ExecutionPlanMetricsSet,
        file_schema: SchemaRef,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
        batch_size: usize,
        predicate: Arc<dyn PhysicalExpr>,
        metadata_db: Arc<MetadataDb>,
    }

    impl NozzleSource {
        pub fn new(
            file_schema: SchemaRef,
            metadata_db: Arc<MetadataDb>,
            predicate: Arc<dyn PhysicalExpr>,
            state: &dyn Session,
        ) -> Self {
            let batch_size = state.config_options().execution.batch_size;
            let table_parquet_options = state.table_options().parquet.clone();
            Self {
                file_schema,
                table_parquet_options,
                metadata_db,
                metrics: ExecutionPlanMetricsSet::default(),
                schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory::default()),
                batch_size,
                predicate,
            }
        }

        pub(super) fn build_pruning_predicates(
            expr: &Arc<dyn PhysicalExpr>,
            file_schema: &SchemaRef,
            predicate_creation_errors: &Count,
        ) -> (
            Option<Arc<PruningPredicate>>,
            Arc<PagePruningAccessPlanFilter>,
        ) {
            let pruning_predicate = PruningPredicate::try_new(expr.clone(), file_schema.clone())
                .map(|pruning_predicate| {
                    // If the pruning predicate is always true, we don't need to use it.
                    pruning_predicate
                        .always_true()
                        .not()
                        .then_some(Arc::new(pruning_predicate))
                })
                .unwrap_or_else(|e| {
                    tracing::error!("Failed to create pruning predicate: {e}");
                    predicate_creation_errors.add(1);
                    None
                });

            let page_pruning_predicate =
                Arc::new(PagePruningAccessPlanFilter::new(&expr, file_schema.clone()));

            (pruning_predicate, page_pruning_predicate)
        }
    }

    impl FileSource for NozzleSource {
        fn create_file_opener(
            &self,
            object_store: Arc<dyn ObjectStore>,
            base_config: &FileScanConfig,
            partition_index: usize,
        ) -> Arc<dyn FileOpener> {
            use TimeUnit::*;
            let projection = base_config
                .file_column_projection_indices()
                .unwrap_or_else(|| (0..base_config.file_schema.fields().len()).collect());

            let coerce_int96 = self
                .table_parquet_options
                .global
                .coerce_int96
                .as_ref()
                .map(|s| match &*s.to_lowercase() {
                    "ns" => Some(Nanosecond),
                    "us" => Some(Microsecond),
                    "ms" => Some(Millisecond),
                    "s" => Some(Second),
                    _ => {
                        tracing::error!(
                            "Unknown or unsupported parquet coerce_int96: \
                    {s}. Valid values are: ns, us, ms, and s."
                        );
                        None
                    }
                })
                .flatten();

            Arc::new(NozzleOpener {
                partition_index,
                projection: Arc::from(projection),
                batch_size: self.batch_size,
                limit: base_config.limit,
                predicate: self.predicate.clone(),
                file_schema: Arc::clone(&base_config.file_schema),
                metrics: self.metrics.clone(),
                pushdown_filters: self.table_parquet_options.global.pushdown_filters,
                reorder_filters: self.table_parquet_options.global.reorder_filters,
                enable_page_index: self.table_parquet_options.global.enable_page_index,
                enable_bloom_filter: self.table_parquet_options.global.bloom_filter_on_read,
                enable_row_group_stats_pruning: self.table_parquet_options.global.pruning,
                schema_adapter_factory: Arc::clone(&self.schema_adapter_factory),
                coerce_int96,
                metadata_db: Arc::clone(&self.metadata_db),
                object_store: Arc::clone(&object_store),
            })
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn file_type(&self) -> &str {
            "parquet"
        }

        fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> FmtResult {
            if self.predicate.eq(&lit(true)) {
                return Ok(());
            }
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    let predicate_creation_errors = Count::new();
                    if let (Some(pruning_predicate), page_pruning_access_plan_filter) =
                        Self::build_pruning_predicates(
                            &self.predicate,
                            &self.file_schema,
                            &predicate_creation_errors,
                        )
                    {
                        let mut guarantees = pruning_predicate
                            .literal_guarantees()
                            .iter()
                            .map(|item| format!("{item}"))
                            .collect::<Vec<_>>();
                        guarantees.sort();
                        let filter_number = page_pruning_access_plan_filter.filter_number();
                        writeln!(
                            f,
                            ", pruning_predicate={}, required_guarantees=[{}], page_pruning_access_plan_length={},",
                            pruning_predicate.predicate_expr(),
                            guarantees.join(", "),
                            filter_number
                        )?;
                    };
                    Ok(())
                }
                DisplayFormatType::TreeRender => {
                    writeln!(f, "predicate={}", fmt_sql(self.predicate.as_ref()))?;
                    Ok(())
                }
            }
        }

        fn metrics(&self) -> &ExecutionPlanMetricsSet {
            &self.metrics
        }

        fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
            Arc::new(Self { ..self.clone() })
        }

        fn statistics(&self) -> DataFusionResult<Statistics> {
            Ok(Statistics::new_unknown(&self.file_schema))
        }

        ///From [`ParquetSource::try_pushdown_filters`](https://github.com/apache/datafusion/blob/f38f52fbf7f9669cf462ca22376694e5f196fe92/datafusion/datasource-parquet/src/source.rs#L619C1-L690C6)
        fn try_pushdown_filters(
            &self,
            filters: Vec<Arc<dyn PhysicalExpr>>,
            config: &ConfigOptions,
        ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn FileSource>>> {
            let config_pushdown_enabled = config.execution.parquet.pushdown_filters;
            let table_pushdown_enabled = self.table_parquet_options.global.pushdown_filters;
            let pushdown_filters = table_pushdown_enabled || config_pushdown_enabled;

            let mut source = self.clone();
            let mut allowed_filters = HashSet::new();
            allowed_filters.reserve(filters.len() + 1);

            let mut remaining_filters = vec![];

            for filter in &filters {
                if can_expr_be_pushed_down_with_schemas(filter, &self.file_schema) {
                    allowed_filters.insert(Arc::clone(filter));
                } else {
                    remaining_filters.push(Arc::clone(filter));
                }
            }

            if allowed_filters.is_empty() {
                return Ok(FilterPushdownPropagation::unsupported(filters));
            }

            allowed_filters.insert(self.predicate.clone());

            source.predicate = conjunction(allowed_filters.iter().cloned());

            let filters = PredicateSupports::new(
                allowed_filters
                    .into_iter()
                    .map(|f| {
                        if pushdown_filters {
                            PredicateSupport::Supported(f)
                        } else {
                            PredicateSupport::Unsupported(f)
                        }
                    })
                    .chain(
                        remaining_filters
                            .into_iter()
                            .map(PredicateSupport::Unsupported),
                    )
                    .collect(),
            );

            Ok(
                FilterPushdownPropagation::with_filters(filters)
                    .with_updated_node(Arc::new(source)),
            )
        }

        fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
            Arc::new(Self {
                batch_size,
                ..self.clone()
            })
        }

        fn with_schema(&self, file_schema: SchemaRef) -> Arc<dyn FileSource> {
            Arc::new(Self {
                file_schema,
                ..self.clone()
            })
        }

        fn with_schema_adapter_factory(
            &self,
            schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
        ) -> DataFusionResult<Arc<dyn FileSource>> {
            Ok(Arc::new(Self {
                schema_adapter_factory,
                ..self.clone()
            }))
        }

        fn with_statistics(&self, _statistics: Statistics) -> Arc<dyn FileSource> {
            Arc::new(Self { ..self.clone() })
        }

        fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
            Some(self.schema_adapter_factory.clone())
        }
    }
}
