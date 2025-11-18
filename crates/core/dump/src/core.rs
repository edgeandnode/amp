use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

pub use block_ranges::{EndBlock, ResolvedEndBlock};
use common::{
    BoxError, LogicalCatalog,
    catalog::physical::{Catalog, PhysicalTable},
    config::Config,
    query_context::Error as QueryError,
    store::Store as DataStore,
};
use dataset_store::{DatasetKind, DatasetStore};
use datasets_derived::DerivedDatasetKind;
use futures::TryStreamExt as _;
use metadata_db::{LocationId, MetadataDb, NotificationMultiplexerHandle};
use monitoring::telemetry::metrics::Meter;
use object_store::ObjectMeta;

use crate::{compaction::AmpCompactor, metrics};

pub mod block_ranges;
mod derived_dataset;
mod raw_dataset;
mod tasks;

/// Dumps a set of tables. All tables must belong to the same dataset.
pub async fn dump_tables(
    ctx: Ctx,
    tables: &[(Arc<PhysicalTable>, Arc<AmpCompactor>)],
    max_writers: u16,
    microbatch_max_interval: u64,
    end: EndBlock,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> Result<(), BoxError> {
    let mut kinds = BTreeSet::new();
    for (t, _) in tables {
        kinds.insert(DatasetKind::from_str(&t.dataset().kind)?);
    }

    if kinds.iter().any(|k| k.is_raw()) {
        if !kinds.iter().all(|k| k.is_raw()) {
            return Err("Cannot mix raw and non-raw datasets in a same dump".into());
        }
        dump_raw_dataset_tables(ctx, tables, max_writers, end, metrics).await
    } else {
        dump_derived_dataset_tables(
            ctx,
            tables,
            microbatch_max_interval,
            max_writers,
            end,
            metrics,
        )
        .await
    }
}

/// Dumps a set of raw dataset tables. All tables must belong to the same dataset.
pub async fn dump_raw_dataset_tables(
    ctx: Ctx,
    tables: &[(Arc<PhysicalTable>, Arc<AmpCompactor>)],
    max_writers: u16,
    end: EndBlock,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> Result<(), BoxError> {
    if tables.is_empty() {
        return Ok(());
    }

    let parquet_opts = crate::parquet_opts(&ctx.config.parquet);

    // Check that all tables belong to the same dataset.
    let dataset = {
        let ds = tables[0].0.table().dataset();
        for (table, _) in tables {
            if table.dataset().manifest_hash != ds.manifest_hash {
                return Err(
                    format!("Table {} is not in {}", table.table_ref(), ds.manifest_hash).into(),
                );
            }
        }
        ds
    };

    let logical = LogicalCatalog::from_tables(tables.iter().map(|(t, _)| t.table()));
    let catalog = Catalog::new(tables.iter().map(|(t, _)| Arc::clone(t)).collect(), logical);

    // Ensure consistency before starting the dump procedure.
    for (table, _) in tables {
        consistency_check(table).await?;
    }

    let kind = DatasetKind::from_str(&dataset.kind)?;
    match kind {
        DatasetKind::EvmRpc | DatasetKind::EthBeacon | DatasetKind::Firehose => {
            raw_dataset::dump(
                ctx,
                max_writers,
                catalog,
                tables,
                parquet_opts,
                end,
                metrics,
                dataset.finalized_blocks_only,
            )
            .await?;
        }
        DatasetKind::Derived => {
            return Err(
                format!("Attempted to dump dataset of kind `{kind}` as raw dataset").into(),
            );
        }
    }

    tracing::info!("dump completed successfully");

    Ok(())
}

/// Dumps a set of derived dataset tables. All tables must belong to the same dataset.
pub async fn dump_derived_dataset_tables(
    ctx: Ctx,
    tables: &[(Arc<PhysicalTable>, Arc<AmpCompactor>)],
    microbatch_max_interval: u64,
    max_writers: u16,
    end: EndBlock,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> Result<(), BoxError> {
    if max_writers > 1 {
        tracing::warn!("max_writers > 1 has no effect for derived datasets");
    }

    let opts = crate::parquet_opts(&ctx.config.parquet);
    let env = ctx.config.make_query_env()?;

    // Pre-check all tables for consistency and validate dataset kinds before spawning tasks
    for (table, _) in tables {
        consistency_check(table).await?;

        let dataset = table.table().dataset();

        if dataset.kind != DerivedDatasetKind {
            return Err(format!(
                "Unsupported dataset kind {} for table {}",
                dataset.kind,
                table.table_ref()
            )
            .into());
        }
    }

    // Process all tables in parallel using FailFastJoinSet
    let mut join_set = tasks::FailFastJoinSet::<Result<(), BoxError>>::new();

    for (table, comppactor) in tables {
        let ctx = ctx.clone();
        let env = env.clone();
        let table = Arc::clone(table);
        let compactor = Arc::clone(comppactor);
        let opts = opts.clone();
        let metrics = metrics.clone();

        join_set.spawn(async move {
            let dataset = table.table().dataset();
            let manifest = ctx
                .dataset_store
                .get_derived_manifest(dataset.manifest_hash())
                .await?;

            derived_dataset::dump_table(
                ctx,
                manifest,
                &env,
                table.clone(),
                compactor,
                &opts,
                microbatch_max_interval,
                end,
                metrics,
            )
            .await?;

            tracing::info!("dump of `{}` completed successfully", table.table_name());
            Ok(())
        });
    }

    // Wait for all tables to complete with fail-fast behavior
    join_set
        .try_wait_all()
        .await
        .map_err(|err| err.into_box_error())?;

    Ok(())
}

/// Dataset dump context
#[derive(Clone)]
pub struct Ctx {
    pub config: Arc<Config>,
    pub metadata_db: MetadataDb,
    pub dataset_store: Arc<DatasetStore>,
    pub data_store: Arc<DataStore>,
    /// Shared notification multiplexer for streaming queries
    pub notification_multiplexer: Arc<NotificationMultiplexerHandle>,
    /// Optional meter for job metrics
    pub meter: Option<Meter>,
}

/// This will check and fix consistency issues when possible. When fixing is not possible, it will
/// return a `CorruptedDataset` error.
///
/// ## List of checks
///
/// Check: All files in the data store are accounted for in the metadata DB.
/// On fail: Fix by deleting orphaned files to restore consistency.
///
/// Check: All files in the table exist in the data store.
/// On fail: Return a `CorruptedDataset` error.
///
/// Check: metadata entries do not contain overlapping ranges.
/// On fail: Return a `CorruptedDataset` error.
pub async fn consistency_check(table: &PhysicalTable) -> Result<(), ConsistencyCheckError> {
    // See also: metadata-consistency

    let location_id = table.location_id();

    let files = table
        .files()
        .await
        .map_err(|err| ConsistencyCheckError::CorruptedTable(location_id, err))?;

    let registered_files: BTreeSet<String> = files.into_iter().map(|m| m.file_name).collect();

    let store = table.object_store();
    let path = table.path();

    let stored_files: BTreeMap<String, ObjectMeta> = store
        .list(Some(table.path()))
        .try_collect::<Vec<ObjectMeta>>()
        .await
        .map_err(|err| ConsistencyCheckError::CorruptedTable(location_id, err.into()))?
        .into_iter()
        .filter_map(|object| Some((object.location.filename()?.to_string(), object)))
        .collect();

    for (filename, object_meta) in &stored_files {
        if !registered_files.contains(filename) {
            // This file was written by a dump job but it is not present in the metadata DB,
            // so it is an orphaned file. Delete it.
            tracing::warn!("Deleting orphaned file: {}", object_meta.location);
            store.delete(&object_meta.location).await?;
        }
    }

    // Check for files in the metadata DB that do not exist in the store.
    for filename in registered_files {
        if !stored_files.contains_key(&filename) {
            let err = format!(
                "file `{path}/{filename}` is registered in metadata DB but is not in the data store"
            )
            .into();
            return Err(ConsistencyCheckError::CorruptedTable(location_id, err));
        }
    }

    Ok(())
}

/// Error type for consistency checks
#[derive(Debug, thiserror::Error)]
#[error("consistency check error: {0}")]
pub enum ConsistencyCheckError {
    #[error("internal query error: {0}")]
    QueryError(#[from] QueryError),

    #[error("object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("table {0} is corrupted: {1}")]
    CorruptedTable(LocationId, BoxError),
}
