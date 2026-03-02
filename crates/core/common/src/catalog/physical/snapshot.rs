use std::{pin::pin, sync::Arc};

use amp_data_store::{DataStore, GetCachedMetadataError};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, memory::DataSourceExec},
    common::{DFSchema, project_schema, stats::Precision},
    datasource::{
        TableProvider, TableType, create_ordering,
        listing::{ListingTableUrl, PartitionedFile},
        object_store::ObjectStoreUrl as DataFusionObjectStoreUrl,
        physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource},
    },
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{ScalarUDF, SortExpr, col, utils::conjunction},
    physical_expr::LexOrdering,
    physical_plan::{ExecutionPlan, PhysicalExpr, empty::EmptyExec},
    prelude::Expr,
};
use datafusion_datasource::compute_all_files_statistics;
use futures::{Stream, StreamExt as _};

use super::{catalog::CatalogTable, reader};
use crate::{
    BlockRange,
    catalog::logical::LogicalTable,
    physical_table::{
        MultiNetworkSegmentsError, SnapshotError, resolved::ResolvedFile,
        snapshot::TableSnapshot as PhyTableSnapshot, table::PhysicalTable,
    },
    sql::TableReference,
};

/// A snapshot of the physical catalog with segment data locked in.
///
/// Provides access to resolved table snapshots for streaming query and
/// segment-level consumers, and exposes resolved-file accessors for the
/// execution layer (via `QueryableSnapshot`).
#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    /// Logical tables describing dataset schemas and metadata.
    tables: Vec<LogicalTable>,
    /// UDFs specific to the datasets corresponding to the resolved tables.
    udfs: Vec<ScalarUDF>,
    /// Each snapshot is paired with its SQL table ref schema string.
    table_snapshots: Vec<(Arc<PhyTableSnapshot>, String)>,
}

impl CatalogSnapshot {
    /// Creates a catalog snapshot by snapshotting every catalog entry.
    ///
    /// When `ignore_canonical_segments` is `true`, canonical chain filtering is
    /// skipped during snapshot creation.
    pub async fn from_catalog(
        tables: Vec<LogicalTable>,
        udfs: Vec<ScalarUDF>,
        entries: &[CatalogTable],
        ignore_canonical_segments: bool,
    ) -> Result<Self, FromCatalogError> {
        let mut table_snapshots = Vec::new();
        for entry in entries {
            let snapshot = entry
                .physical_table()
                .snapshot(ignore_canonical_segments)
                .await
                .map_err(FromCatalogError)?;
            table_snapshots.push((Arc::new(snapshot), entry.sql_schema_name().to_string()));
        }

        Ok(Self {
            tables,
            udfs,
            table_snapshots,
        })
    }

    /// Returns the logical tables.
    pub fn tables(&self) -> &[LogicalTable] {
        &self.tables
    }

    /// Returns the table snapshots paired with their SQL table ref schema strings.
    pub fn table_snapshots(&self) -> impl Iterator<Item = (&Arc<PhyTableSnapshot>, &str)> {
        self.table_snapshots
            .iter()
            .map(|(s, sql)| (s, sql.as_str()))
    }

    /// Returns the user-defined functions.
    pub fn udfs(&self) -> &[ScalarUDF] {
        &self.udfs
    }

    /// Reconstructs `CatalogTable` entries from the snapshotted data.
    ///
    /// Useful for rebuilding a `Catalog` from a snapshot (e.g. for forked-chain contexts).
    // TODO: Revisit. Can reconstruct by any other means?
    pub fn catalog_entries(&self) -> Vec<CatalogTable> {
        self.table_snapshots
            .iter()
            .map(|(s, sql)| CatalogTable::new(s.physical_table().clone(), sql.clone()))
            .collect()
    }
}

/// Errors that can occur when creating a catalog snapshot from a catalog.
#[derive(Debug, thiserror::Error)]
#[error("failed to create catalog snapshot")]
pub struct FromCatalogError(#[source] pub SnapshotError);

/// The query execution wrapper over a resolved physical table.
///
/// Wraps `physical_table::TableSnapshot` with DataFusion execution concerns.
/// Does not import `Segment`, `Chain`, or `canonical_chain` — works entirely with
/// [`ResolvedFile`] values produced by `physical_table::TableSnapshot`.
#[derive(Debug, Clone)]
pub struct QueryableSnapshot {
    /// The underlying physical table providing storage access.
    physical_table: Arc<PhysicalTable>,
    /// Parquet files resolved from the table snapshot's canonical chain.
    resolved_files: Vec<ResolvedFile>,
    /// The contiguous block range covered by synced data, if any.
    synced_range: Option<BlockRange>,
    /// Factory for creating parquet readers with caching and store access.
    reader_factory: Arc<reader::AmpReaderFactory>,
    /// The dataset reference portion of SQL table references (e.g. `anvil_rpc`).
    sql_schema_name: String,
}

impl QueryableSnapshot {
    /// Construct from a `physical_table::TableSnapshot`, creating the reader factory
    /// from the provided `store`.
    ///
    /// # Errors
    /// Returns [`MultiNetworkSegmentsError`] if the snapshot contains multi-network
    /// segments. See [`physical_table::snapshot::TableSnapshot::synced_range`] for details.
    pub fn from_snapshot(
        snapshot: &PhyTableSnapshot,
        store: DataStore,
        sql_schema_name: String,
    ) -> Result<Self, MultiNetworkSegmentsError> {
        let reader_factory = Arc::new(reader::AmpReaderFactory {
            location_id: snapshot.physical_table().location_id(),
            store,
            schema: snapshot.physical_table().schema(),
        });
        Ok(Self {
            physical_table: snapshot.physical_table().clone(),
            resolved_files: snapshot.resolved_files(),
            synced_range: snapshot.synced_range()?,
            reader_factory,
            sql_schema_name,
        })
    }

    /// Returns the contiguous block range covered by synced data, or `None` if empty.
    pub fn synced_range(&self) -> Option<BlockRange> {
        self.synced_range.clone()
    }

    /// Returns a reference to the underlying physical table.
    pub fn physical_table(&self) -> &Arc<PhysicalTable> {
        &self.physical_table
    }

    /// Returns the dataset reference portion for SQL table references.
    pub fn sql_schema_name(&self) -> &str {
        &self.sql_schema_name
    }

    /// Qualified table reference in the format `dataset_name.table_name`.
    pub fn table_ref(&self) -> TableReference {
        TableReference::partial(
            self.sql_schema_name.clone(),
            self.physical_table.table_name().clone(),
        )
    }

    /// Builds sort expressions for each field that appears in the table's `sorted_by` list.
    fn order_exprs(&self) -> Vec<Vec<SortExpr>> {
        let sorted_by = self.physical_table.table().sorted_by();
        self.physical_table
            .schema()
            .fields()
            .iter()
            .filter_map(move |field| {
                if sorted_by.contains(field.name()) {
                    Some(vec![SortExpr::new(col(field.name()), true, false)])
                } else {
                    None
                }
            })
            .collect()
    }

    /// Derives the DataFusion object store URL from the physical table's base URL.
    fn object_store_url(&self) -> DataFusionResult<DataFusionObjectStoreUrl> {
        Ok(ListingTableUrl::try_new(self.physical_table.url().clone(), None)?.object_store())
    }

    /// Computes the lexicographic output ordering from the table's sort expressions.
    fn output_ordering(&self) -> DataFusionResult<Vec<LexOrdering>> {
        let schema = self.physical_table.schema();
        let sort_order = self.order_exprs();
        create_ordering(&schema, &sort_order)
    }

    /// Converts logical filter expressions into a single physical predicate.
    ///
    /// Combines all filters via conjunction; defaults to `true` when no filters are provided.
    fn filters_to_predicate(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let df_schema = DFSchema::try_from(self.physical_table.schema())?;
        let predicate = conjunction(filters.to_vec());
        let predicate = predicate
            .map(|predicate| state.create_physical_expr(predicate, &df_schema))
            .transpose()?
            .unwrap_or_else(|| datafusion::physical_expr::expressions::lit(true));
        Ok(predicate)
    }

    /// Converts a resolved file into a DataFusion `PartitionedFile` with cached metadata.
    async fn to_partitioned_file(
        &self,
        file: &ResolvedFile,
    ) -> Result<PartitionedFile, GetCachedMetadataError> {
        let metadata = self.reader_factory.get_cached_metadata(file.id).await?;
        let pf = PartitionedFile::from(file.object.clone())
            .with_extensions(Arc::new(file.id))
            .with_statistics(metadata.statistics);
        Ok(pf)
    }
}

#[async_trait::async_trait]
impl TableProvider for QueryableSnapshot {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.physical_table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    #[tracing::instrument(skip_all, err, fields(table = %self.table_ref(), files = %self.resolved_files.len()))]
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        tracing::debug!("creating scan execution plan");

        if self.synced_range.is_none() {
            // This is necessary to work around empty tables tripping the DF sanity checker
            tracing::debug!("table has no synced data, returning empty execution plan");
            let projected_schema = project_schema(&self.schema(), projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let target_partitions = state.config_options().execution.target_partitions;
        let table_schema = self.physical_table.schema();
        let (file_groups, statistics) = {
            let file_count = self.resolved_files.len();
            let file_stream = futures::stream::iter(self.resolved_files.iter())
                .then(|f| self.to_partitioned_file(f));
            let partitioned = round_robin(file_stream, file_count, target_partitions)
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;
            compute_all_files_statistics(partitioned, table_schema.clone(), true, false)?
        };
        if statistics.num_rows == Precision::Absent {
            // This log likely signifies a bug in our statistics fetching.
            tracing::warn!("Table has no row count statistics. Queries may be inefficient.");
        }

        let output_ordering = self.output_ordering()?;
        let object_store_url = self.object_store_url()?;
        let predicate = self.filters_to_predicate(state, filters)?;

        let parquet_file_reader_factory = Arc::clone(&self.reader_factory);
        let table_parquet_options = state.table_options().parquet.clone();
        let file_source = Arc::new(
            ParquetSource::new(table_schema)
                .with_table_parquet_options(table_parquet_options)
                .with_parquet_file_reader_factory(parquet_file_reader_factory)
                .with_predicate(predicate),
        );

        let data_source = Arc::new(
            FileScanConfigBuilder::new(object_store_url, file_source)
                .with_file_groups(file_groups)
                .with_limit(limit)
                .with_output_ordering(output_ordering)
                .with_projection_indices(projection.cloned())?
                .with_statistics(statistics)
                .build(),
        );

        Ok(Arc::new(DataSourceExec::new(data_source)))
    }
}

/// Distributes files across `target_partitions` file groups in round-robin order.
///
/// Creates `min(files_len, target_partitions)` groups and assigns each file to
/// `groups[index % size]`, balancing files evenly across partitions.
async fn round_robin(
    files: impl Stream<Item = Result<PartitionedFile, GetCachedMetadataError>> + Send,
    files_len: usize,
    target_partitions: usize,
) -> Result<Vec<FileGroup>, GetCachedMetadataError> {
    let size = files_len.min(target_partitions);
    let mut groups = vec![FileGroup::default(); size];
    let mut stream = pin!(files.enumerate());
    while let Some((idx, file)) = stream.next().await {
        groups[idx % size].push(file?);
    }
    Ok(groups)
}
