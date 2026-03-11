use std::{collections::BTreeMap, sync::Arc, time::Instant};

use amp_worker_core::{
    EndBlock, ResolvedEndBlock, WriterProperties,
    block_ranges::{GetLatestBlockError, ResolutionError, resolve_end_block},
    compaction::AmpCompactor,
    error_detail::ErrorDetailsProvider,
    progress::{SyncCompletedInfo, SyncFailedInfo, SyncStartedInfo},
    retryable::RetryableErrorExt,
    tasks::{self, TryWaitAllError},
};
use common::{
    BlockNum,
    amp_catalog_provider::{AMP_CATALOG_NAME, AmpCatalogProvider},
    catalog::{
        logical::LogicalTable,
        physical::{EarliestBlockError, for_dump as physical_for_dump},
    },
    context::{exec::ExecContextBuilder, plan::PlanContextBuilder},
    cursor::Cursor,
    datasets_cache::ResolveRevisionError,
    exec_env::ExecEnv,
    physical_table::{CanonicalChainError, PhysicalTable},
    retryable::RetryableErrorExt as _,
    self_schema_provider::SelfSchemaProvider,
    sql::{ParseSqlError, ResolveTableReferencesError, TableReference, resolve_table_references},
};
use datasets_common::{hash_reference::HashReference, table_name::TableName};
use datasets_derived::{
    Manifest as DerivedManifest,
    deps::{DepAlias, DepAliasOrSelfRef, DepAliasOrSelfRefError, SELF_REF_KEYWORD},
    manifest::TableInput,
};
use tracing::Instrument as _;

use super::query::{MaterializeSqlQueryError, materialize_sql_query};
use crate::job_ctx::Context;

/// Partitions table references into external dependency refs and self-ref table names.
///
/// References with `self.` schema are collected as self-ref table names.
/// All other qualified references are converted to external `DepAlias` refs.
fn partition_table_refs(
    refs: Vec<TableReference<DepAliasOrSelfRef>>,
) -> (Vec<TableReference<DepAlias>>, Vec<TableName>) {
    let mut ext_refs: Vec<TableReference<DepAlias>> = Vec::new();
    let mut self_ref_tables: Vec<TableName> = Vec::new();

    for table_ref in refs {
        match table_ref {
            TableReference::Bare { table } => {
                ext_refs.push(TableReference::Bare { table });
            }
            TableReference::Partial { schema, table } => match schema.as_ref() {
                DepAliasOrSelfRef::SelfRef => {
                    self_ref_tables.push(table.as_ref().clone());
                }
                DepAliasOrSelfRef::DepAlias(alias) => {
                    ext_refs.push(TableReference::Partial {
                        schema: Arc::new(alias.clone()),
                        table,
                    });
                }
            },
        }
    }

    (ext_refs, self_ref_tables)
}

/// Materializes a derived dataset table
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(table = %table.table_name()), err)]
pub async fn materialize_table(
    ctx: Context,
    manifest: &DerivedManifest,
    env: ExecEnv,
    table: Arc<PhysicalTable>,
    compactor: Arc<AmpCompactor>,
    opts: Arc<WriterProperties>,
    end: EndBlock,
    siblings: &BTreeMap<TableName, Arc<PhysicalTable>>,
) -> Result<(), MaterializeTableError> {
    let materialize_start_time = Instant::now();

    let table_name = table.table_name().to_string();

    // Clone values needed for metrics after async block
    let table_name_for_metrics = table_name.clone();
    let metrics_for_after = ctx.metrics.clone();

    // Get the table definition from the manifest
    let table_def = manifest
        .tables
        .get(table.table_name())
        .ok_or_else(|| MaterializeTableError::TableNotFound(table_name.clone()))?;

    // Extract SQL query from the table input
    let query_sql = match &table_def.input {
        TableInput::View(view) => &view.sql,
    };

    // Parse the SQL query
    let query = common::sql::parse(query_sql).map_err(MaterializeTableError::ParseSql)?;

    // Resolve all dependencies from the manifest to HashReference
    // This ensures SQL schema references map to the exact dataset versions
    // specified in the manifest's dependencies
    let mut dependencies: BTreeMap<DepAlias, HashReference> = BTreeMap::new();

    for (alias, dep_reference) in &manifest.dependencies {
        // Convert DepReference to Reference for resolution
        let reference = dep_reference.to_reference();

        // Resolve reference to its manifest hash
        let hash_reference = ctx
            .datasets_cache
            .resolve_revision(&reference)
            .await
            .map_err(MaterializeTableError::ResolveRevision)?
            .ok_or_else(|| MaterializeTableError::DependencyNotFound {
                alias: alias.to_string(),
                reference: reference.to_string(),
            })?;

        dependencies.insert(alias.clone(), hash_reference);
    }

    let mut join_set = tasks::FailFastJoinSet::<Result<(), MaterializeTableSpawnError>>::new();

    let self_schema_provider = SelfSchemaProvider::from_manifest_udfs(&manifest.functions);

    // Resolve and partition table references into external deps and self-refs.
    let all_table_refs = resolve_table_references::<DepAliasOrSelfRef>(&query)
        .map_err(MaterializeTableError::ResolveTableReferences)?;
    let (ext_refs, self_ref_tables) = partition_table_refs(all_table_refs);

    // Resolve self-ref sibling tables for planning (schema) and execution (physical)
    let mut pre_resolved = Vec::new();
    for self_ref_name in &self_ref_tables {
        let sibling = siblings
            .get(self_ref_name)
            .ok_or_else(|| MaterializeTableError::SelfRefTableNotFound(self_ref_name.clone()))?;

        // Planning: register schema so DataFusion knows column types
        self_schema_provider.add_table(self_ref_name.to_string(), sibling.table().schema().clone());

        // Execution: build resolved entry so attach() can find real data
        pre_resolved.push(physical_for_dump::ResolvedTableEntry {
            logical: LogicalTable::new(
                SELF_REF_KEYWORD.to_string(),
                sibling.dataset_reference().clone(),
                sibling.table().clone(),
            ),
            physical: Arc::clone(sibling),
            schema_name: Arc::from(SELF_REF_KEYWORD),
        });
    }

    // Resolve external deps, combine with self-ref entries, and build the catalog
    let mut all_entries = physical_for_dump::resolve_external_deps(
        &ctx.datasets_cache,
        &ctx.data_store,
        &dependencies,
        ext_refs,
    )
    .await
    .map_err(MaterializeTableError::CreatePhysicalCatalog)?;

    all_entries.extend(pre_resolved);

    let catalog = physical_for_dump::build_catalog(
        all_entries,
        self_schema_provider.udfs().to_vec(),
        &dependencies,
    );

    let dep_alias_map = catalog.dep_aliases().clone();

    // Planning context: tables resolved lazily by AmpCatalogProvider with dep aliases.
    // UDFs are kept in the self-schema provider for self-refs and eth_call.
    let self_schema: Arc<dyn common::amp_catalog_provider::AsyncSchemaProvider> =
        Arc::new(self_schema_provider);
    let amp_catalog = Arc::new(
        AmpCatalogProvider::new(ctx.datasets_cache.clone(), ctx.ethcall_udfs_cache.clone())
            .with_dep_aliases(dep_alias_map)
            .with_self_schema(self_schema),
    );
    let planning_ctx = PlanContextBuilder::new(env.session_config.clone())
        .with_table_catalog(AMP_CATALOG_NAME, amp_catalog.clone())
        .with_func_catalog(AMP_CATALOG_NAME, amp_catalog)
        .build();

    join_set.spawn(
        async move {
            let plan = planning_ctx
                .statement_to_plan(query.clone())
                .await
                .map_err(MaterializeTableSpawnError::PlanSql)?;
            if let Err(err) = plan.is_incremental() {
                return Err(MaterializeTableSpawnError::NonIncrementalQuery {
                    table_name,
                    source: err,
                });
            }

            // Wait until at least one dependency has synced data, then use its
            // earliest block as the start. For tables with only external deps this
            // returns immediately. For tables with self-refs, this blocks on the
            // notification pipeline until the sibling table produces data.
            // No explicit timeout: if a sibling fails, FailFastJoinSet aborts this task.
            let start = {
                let mut receivers: BTreeMap<_, _> = BTreeMap::new();
                for pt in catalog.physical_tables() {
                    let loc = pt.location_id();
                    receivers
                        .entry(loc)
                        .or_insert(ctx.notification_multiplexer.subscribe(loc).await);
                }

                loop {
                    match catalog
                        .earliest_block()
                        .await
                        .map_err(MaterializeTableSpawnError::EarliestBlock)?
                    {
                        Some(block) => break block,
                        None => {
                            tracing::debug!(
                                "dependencies not yet synced, blocking on notification"
                            );
                            let futs: Vec<_> = receivers
                                .values_mut()
                                .map(|rx| Box::pin(rx.changed()))
                                .collect();
                            let _ = futures::future::select_all(futs).await;
                        }
                    }
                }
            };

            let resolved = resolve_end_block(&end, start, async {
                let query_ctx = ExecContextBuilder::new(env.clone())
                    .with_isolate_pool(ctx.isolate_pool.clone())
                    .for_catalog(catalog.clone(), false)
                    .await?;
                let max_end_blocks = query_ctx
                    .max_end_blocks(&plan.clone().attach_to(&query_ctx)?)
                    .await?;
                // For now, all materialized tables only support single networks.
                assert_eq!(max_end_blocks.len(), 1);
                Ok::<Option<BlockNum>, GetLatestBlockError>(max_end_blocks.into_values().next())
            })
            .await
            .map_err(MaterializeTableSpawnError::ResolveEndBlock)?;

            let end = match resolved {
                ResolvedEndBlock::Continuous => None,
                ResolvedEndBlock::NoDataAvailable => {
                    tracing::warn!(
                        "no blocks to materialize for {table_name}, dependencies are empty"
                    );
                    return Ok::<(), MaterializeTableSpawnError>(());
                }
                ResolvedEndBlock::Block(block) => Some(block),
            };

            // Emit sync.started event now that we have resolved start/end blocks
            if let Some(ref reporter) = ctx.progress_reporter {
                reporter.report_sync_started(SyncStartedInfo {
                    table_name: table.table_name().clone(),
                    start_block: Some(start),
                    end_block: end,
                });
            }

            // Track start time for duration calculation
            let table_materialize_start = Instant::now();

            let cursor = table
                .canonical_chain()
                .await
                .map_err(MaterializeTableSpawnError::CanonicalChain)?
                .map(|c| Cursor::from_ranges(c.last_ranges()));

            // Execute the materialization, capturing any errors for sync.failed event
            let materialize_result = materialize_sql_query(
                &ctx,
                &env,
                &ctx.isolate_pool,
                &catalog,
                plan.clone(),
                start,
                end,
                cursor,
                table.clone(),
                compactor,
                &opts,
            )
            .await;

            // Handle materialization result and emit appropriate lifecycle event
            if let Err(ref err) = materialize_result {
                if let Some(ref reporter) = ctx.progress_reporter {
                    reporter.report_sync_failed(SyncFailedInfo {
                        table_name: table.table_name().clone(),
                        error_message: err.to_string(),
                        error_type: Some("DerivedMaterializeError".to_string()),
                    });
                }
                return materialize_result.map_err(MaterializeTableSpawnError::MaterializeSqlQuery);
            }

            // Emit sync.completed event for bounded jobs only
            // (continuous jobs never complete, they just keep syncing)
            if let Some(final_block) = end
                && let Some(ref reporter) = ctx.progress_reporter
            {
                reporter.report_sync_completed(SyncCompletedInfo {
                    table_name: table.table_name().clone(),
                    final_block,
                    duration_millis: table_materialize_start.elapsed().as_millis() as u64,
                });
            }

            Ok(())
        }
        .in_current_span(),
    );

    // Wait for all the jobs to finish, returning an error if any job panics or fails
    if let Err(err) = join_set.try_wait_all().await {
        tracing::error!(table=%table_name_for_metrics, error=%err, "dataset materialization failed");

        // Record error metrics
        if let Some(ref metrics) = metrics_for_after {
            metrics.record_dump_error(table_name_for_metrics.to_string());
        }

        return Err(MaterializeTableError::ParallelTaskFailed(err));
    }

    // Record materialization duration on successful completion
    if let Some(ref metrics) = metrics_for_after {
        let duration_millis = materialize_start_time.elapsed().as_millis() as f64;
        metrics.record_dump_duration(duration_millis, table_name_for_metrics.to_string());
    }

    Ok(())
}

/// Errors that occur when materializing a derived dataset table
///
/// This error type is used by `materialize_table()`.
#[derive(Debug, thiserror::Error)]
pub enum MaterializeTableError {
    /// Table definition not found in the dataset manifest
    ///
    /// This occurs when the requested table name does not exist in the
    /// manifest's table definitions.
    #[error("table `{0}` not found in dataset")]
    TableNotFound(String),

    /// Failed to parse the SQL query from the table definition
    ///
    /// This occurs when the SQL string in the table's input definition
    /// cannot be parsed as valid SQL.
    #[error("failed to parse SQL query: {0}")]
    ParseSql(#[source] ParseSqlError),

    /// Failed to resolve a dataset revision reference
    ///
    /// This occurs when looking up a dependency's revision (tag, hash, or version)
    /// in the dataset store fails.
    #[error("failed to resolve revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// A dependency dataset was not found
    ///
    /// This occurs when a dependency alias in the manifest references a dataset
    /// that does not exist or cannot be resolved.
    #[error("failed to resolve dependency '{alias}' ({reference})")]
    DependencyNotFound { alias: String, reference: String },

    /// Failed to resolve table references from the SQL query
    ///
    /// This occurs when extracting and resolving table references from the
    /// parsed SQL query fails.
    #[error("failed to resolve table references: {0}")]
    ResolveTableReferences(#[source] ResolveTableReferencesError<DepAliasOrSelfRefError>),

    /// Failed to create the physical catalog for query execution
    ///
    /// This occurs when building the physical catalog from the logical
    /// catalog fails.
    #[error("failed to create physical catalog: {0}")]
    CreatePhysicalCatalog(#[source] physical_for_dump::CreateCatalogError),

    /// A self-ref table was not found among sibling tables
    ///
    /// This occurs when a SQL query references `self.table_name` but no sibling
    /// table with that name exists in the dataset.
    #[error("self-referenced table '{0}' not found in dataset")]
    SelfRefTableNotFound(TableName),

    /// A parallel materialization task failed during execution
    ///
    /// This occurs when one of the parallel tasks spawned for materializing table data
    /// fails or panics. The fail-fast behavior ensures all other tasks are aborted
    /// when this happens.
    #[error("parallel materialization task failed: {0}")]
    ParallelTaskFailed(#[source] TryWaitAllError<MaterializeTableSpawnError>),
}

impl ErrorDetailsProvider for MaterializeTableError {
    fn detail_source(&self) -> Option<&dyn ErrorDetailsProvider> {
        match self {
            Self::ParallelTaskFailed(err) => Some(err),
            _ => None,
        }
    }
}

impl RetryableErrorExt for MaterializeTableError {
    fn is_retryable(&self) -> bool {
        match self {
            // Config/definition errors — fatal
            Self::TableNotFound(_) => false,
            Self::ParseSql(_) => false,
            Self::DependencyNotFound { .. } => false,
            Self::ResolveTableReferences(_) => false,
            Self::CreatePhysicalCatalog(_) => false,
            Self::SelfRefTableNotFound(_) => false,

            // Transient DB failure — recoverable
            Self::ResolveRevision(err) => err.0.is_retryable(),

            // Parallel tasks — delegate to TryWaitAllError classification
            Self::ParallelTaskFailed(err) => err.is_retryable(),
        }
    }
}

/// Errors that occur in the spawned async task for table materialization
///
/// This error type is used by the async task spawned in `materialize_table()`.
#[derive(Debug, thiserror::Error)]
pub enum MaterializeTableSpawnError {
    /// Failed to plan the SQL query for execution
    ///
    /// This occurs when DataFusion cannot create an execution plan
    /// from the parsed SQL query.
    #[error("failed to plan SQL query: {0}")]
    PlanSql(#[source] datafusion::error::DataFusionError),

    /// The query is not incremental and cannot be synced
    ///
    /// This occurs when the SQL query cannot be incrementalized,
    /// which is required for derived dataset syncing.
    #[error("syncing table {table_name} is not supported: {source}")]
    NonIncrementalQuery {
        table_name: String,
        #[source]
        source: common::incrementalizer::NonIncrementalQueryError,
    },

    /// Failed to resolve the end block for materialization
    ///
    /// This occurs when determining the target end block for the
    /// materialization operation fails.
    #[error("failed to resolve end block: {0}")]
    ResolveEndBlock(#[source] ResolutionError),

    /// Failed to get the canonical chain from the physical table
    ///
    /// This occurs when querying the table's canonical chain for
    /// resume watermark calculation fails.
    #[error("failed to get canonical chain: {0}")]
    CanonicalChain(#[source] CanonicalChainError),

    /// Failed to compute the earliest block from dependencies
    ///
    /// This occurs when snapshotting dependency tables to determine their
    /// earliest synced block fails.
    #[error("failed to compute earliest block: {0}")]
    EarliestBlock(#[source] EarliestBlockError),

    /// Failed to execute the SQL query materialization operation
    ///
    /// This occurs when the inner `materialize_sql_query` function fails.
    #[error("failed to materialize SQL query: {0}")]
    MaterializeSqlQuery(#[source] MaterializeSqlQueryError),
}

impl ErrorDetailsProvider for MaterializeTableSpawnError {
    fn detail_source(&self) -> Option<&dyn ErrorDetailsProvider> {
        match self {
            Self::MaterializeSqlQuery(err) => Some(err),
            _ => None,
        }
    }
}

impl RetryableErrorExt for MaterializeTableSpawnError {
    fn is_retryable(&self) -> bool {
        match self {
            // SQL/query structure errors — fatal
            Self::PlanSql(_) => false,
            Self::NonIncrementalQuery { .. } => false,

            // Delegate to inner error classification
            Self::CanonicalChain(err) => err.is_retryable(),
            Self::EarliestBlock(err) => err.is_retryable(),

            // Block range resolution — inspect the source variant
            Self::ResolveEndBlock(err) => err.is_retryable(),

            // Delegate to MaterializeSqlQueryError classification
            Self::MaterializeSqlQuery(err) => err.is_retryable(),
        }
    }
}

#[cfg(test)]
mod tests {
    use common::sql::resolve_table_references;
    use datasets_derived::{deps::DepAliasOrSelfRef, sql_str::SqlStr};

    use super::*;

    fn parse_and_partition(sql: &str) -> (Vec<TableReference<DepAlias>>, Vec<TableName>) {
        let sql_str: SqlStr = sql.parse().expect("sql should parse to SqlStr");
        let stmt = common::sql::parse(&sql_str).expect("sql should parse to statement");
        let refs = resolve_table_references::<DepAliasOrSelfRef>(&stmt)
            .expect("table references should resolve");
        partition_table_refs(refs)
    }

    #[test]
    fn partition_table_refs_with_only_external_deps_returns_ext_refs_only() {
        //* When
        let (ext, self_refs) = parse_and_partition("SELECT * FROM eth_firehose.blocks");

        //* Then
        assert_eq!(ext.len(), 1, "should have one external ref");
        assert!(self_refs.is_empty(), "should have no self-refs");
        assert_eq!(ext[0].to_string(), "eth_firehose.blocks");
    }

    #[test]
    fn partition_table_refs_with_only_self_refs_returns_self_ref_tables_only() {
        //* When
        let (ext, self_refs) = parse_and_partition("SELECT * FROM self.blocks_base");

        //* Then
        assert!(ext.is_empty(), "should have no external refs");
        assert_eq!(self_refs.len(), 1, "should have one self-ref");
        assert_eq!(self_refs[0].to_string(), "blocks_base");
    }

    #[test]
    fn partition_table_refs_with_mixed_refs_splits_correctly() {
        //* When
        let (ext, self_refs) = parse_and_partition(
            "SELECT a.block_num, b.hash FROM eth_firehose.blocks a JOIN self.blocks_base b ON a.block_num = b.block_num",
        );

        //* Then
        assert_eq!(ext.len(), 1, "should have one external ref");
        assert_eq!(self_refs.len(), 1, "should have one self-ref");
        assert_eq!(ext[0].to_string(), "eth_firehose.blocks");
        assert_eq!(self_refs[0].to_string(), "blocks_base");
    }

    #[test]
    fn partition_table_refs_with_multiple_self_refs_captures_all() {
        //* When
        let (ext, self_refs) = parse_and_partition(
            "SELECT * FROM self.blocks_base JOIN self.transactions ON self.blocks_base.block_num = self.transactions.block_num",
        );

        //* Then
        assert!(ext.is_empty(), "should have no external refs");
        assert_eq!(self_refs.len(), 2, "should have two self-refs");
        assert!(
            self_refs.iter().any(|t| *t == "blocks_base"),
            "should contain blocks_base"
        );
        assert!(
            self_refs.iter().any(|t| *t == "transactions"),
            "should contain transactions"
        );
    }

    #[test]
    fn is_retryable_with_self_ref_table_not_found_returns_false() {
        //* Given
        let err = MaterializeTableError::SelfRefTableNotFound(
            "missing_table".parse().expect("should parse table name"),
        );

        //* When
        let result = err.is_retryable();

        //* Then
        assert!(!result, "SelfRefTableNotFound should not be retryable");
    }
}
