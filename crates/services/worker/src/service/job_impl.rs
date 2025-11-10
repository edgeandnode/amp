//! Internal job implementation for the worker service.

use std::sync::Arc;

use common::{
    BoxError,
    catalog::{JobLabels, physical::PhysicalTable},
};
use datasets_common::{
    hash::Hash, reference::Reference, revision::Revision, table_name::TableName,
};
use dump::{Ctx, metrics::MetricsRegistry};
use tracing::{Instrument, info_span};

use crate::job::{JobDescriptor, JobId};

/// Create and run a worker job that dumps tables from a dataset.
///
/// This function performs initialization (fetching metadata and building physical tables)
/// and returns a future that executes the dump operation.
pub(super) async fn new(
    ctx: Ctx,
    job_id: JobId,
    job_desc: JobDescriptor,
) -> Result<impl Future<Output = Result<(), BoxError>>, JobInitError> {
    let (end_block, max_writers, dataset_namespace, dataset_name, manifest_hash) = match job_desc {
        JobDescriptor::Dump {
            end_block,
            max_writers,
            dataset_namespace,
            dataset_name,
            manifest_hash,
        } => (
            end_block,
            max_writers,
            dataset_namespace,
            dataset_name,
            manifest_hash,
        ),
    };

    let output_locations = metadata_db::physical_table::get_by_job_id(&ctx.metadata_db, job_id)
        .await
        .map_err(JobInitError::FetchOutputLocations)?;

    let dataset_ref = Reference::new(
        dataset_namespace.clone(),
        dataset_name.clone(),
        Revision::Hash(manifest_hash.clone()),
    );

    let job_labels = JobLabels {
        dataset_namespace: dataset_namespace.clone(),
        dataset_name: dataset_name.clone(),
        manifest_hash: manifest_hash.clone(),
    };
    let metrics = ctx
        .meter
        .as_ref()
        .map(|m| Arc::new(MetricsRegistry::new(m, job_labels.clone())));

    let mut tables = vec![];
    for location in output_locations {
        let hash: Hash = location.manifest_hash.into();

        let dataset = ctx
            .dataset_store
            .get_dataset_by_hash(&hash)
            .await
            .map_err(|err| JobInitError::FetchDataset {
                hash: hash.clone(),
                source: err,
            })?
            .ok_or_else(|| JobInitError::DatasetNotFound { hash: hash.clone() })?;

        let mut resolved_tables = dataset.resolved_tables(dataset_ref.clone().into());
        let Some(table) = resolved_tables.find(|t| t.name() == location.table_name) else {
            return Err(JobInitError::TableNotFound {
                table_name: location.table_name.into(),
                dataset_hash: hash,
            });
        };

        tables.push(
            PhysicalTable::new(
                table.clone(),
                location.url,
                location.id,
                ctx.metadata_db.clone(),
                job_labels.clone(),
            )
            .map_err(|err| JobInitError::CreatePhysicalTable {
                table_name: table.name().clone(),
                source: err,
            })?
            .into(),
        );
    }

    let microbatch_max_interval = ctx.config.microbatch_max_interval;
    let fut = async move {
        dump::dump_tables(
            ctx,
            &tables,
            max_writers,
            microbatch_max_interval,
            end_block,
            metrics,
        )
        .instrument(info_span!("dump_job", %job_id, dataset = %dataset_ref))
        .await
    };
    Ok(fut)
}

/// Errors that occur during job initialization
///
/// This error type is used by the job initialization phase which fetches
/// metadata and builds physical tables before starting the actual dump.
#[derive(Debug, thiserror::Error)]
pub enum JobInitError {
    /// Failed to fetch output locations from metadata database
    ///
    /// This error occurs when querying the physical_table records associated
    /// with a job ID fails, typically due to database connection issues or
    /// the job not having any registered output locations.
    #[error("Failed to fetch output locations from metadata database")]
    FetchOutputLocations(#[source] metadata_db::Error),

    /// Failed to fetch dataset from dataset store
    ///
    /// This error occurs when retrieving a dataset by its manifest hash fails.
    /// The dataset store may be unavailable or the manifest may not be cached.
    ///
    /// Note: This wraps BoxError because dataset_store::get_by_hash currently
    /// returns BoxError. This should be replaced with a concrete error type.
    #[error("Failed to fetch dataset with hash '{hash}'")]
    FetchDataset {
        hash: Hash,
        #[source]
        source: dataset_store::GetDatasetByHashError,
    },

    /// Dataset not found in dataset store
    ///
    /// This error occurs when a manifest hash referenced in the output locations
    /// does not correspond to any dataset in the store. This indicates a data
    /// inconsistency between the metadata database and the dataset store.
    #[error("Dataset not found: {hash}")]
    DatasetNotFound { hash: Hash },

    /// Table not found in dataset
    ///
    /// This error occurs when a table name from the output locations does not
    /// match any table defined in the dataset manifest. This indicates a
    /// mismatch between the job's expected outputs and the actual dataset schema.
    #[error("Table '{table_name}' not found in dataset '{dataset_hash}'")]
    TableNotFound {
        table_name: TableName,
        dataset_hash: Hash,
    },

    /// Failed to create physical table
    ///
    /// This error occurs when constructing a PhysicalTable instance fails,
    /// typically due to invalid URL parsing or object store configuration issues.
    ///
    /// Note: This wraps BoxError because PhysicalTable::new currently returns
    /// BoxError. This should be replaced with a concrete error type.
    #[error("Failed to create physical table for '{table_name}'")]
    CreatePhysicalTable {
        table_name: TableName,
        #[source]
        source: BoxError,
    },
}
