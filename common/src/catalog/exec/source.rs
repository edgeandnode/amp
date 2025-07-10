use std::{
    collections::HashSet,
    fmt::{Formatter, Result as FmtResult},
    ops::Not,
    sync::Arc,
};

use datafusion::{
    arrow::datatypes::{SchemaRef, TimeUnit},
    catalog::Session,
    common::Statistics,
    config::{ConfigOptions, TableParquetOptions},
    datasource::{
        physical_plan::{
            FileOpener, FileScanConfig, FileSource,
            parquet::{PagePruningAccessPlanFilter, can_expr_be_pushed_down_with_schemas},
        },
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory},
    },
    error::Result as DataFusionResult,
    physical_expr::conjunction,
    physical_expr_common::physical_expr::fmt_sql,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        DisplayFormatType, PhysicalExpr,
        expressions::lit,
        filter_pushdown::{FilterPushdownPropagation, PredicateSupport, PredicateSupports},
        metrics::{Count, ExecutionPlanMetricsSet},
    },
};
use metadata_db::MetadataDb;
use object_store::ObjectStore;

use crate::catalog::exec::opener::NozzleOpener;

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

        Ok(FilterPushdownPropagation::with_filters(filters).with_updated_node(Arc::new(source)))
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
