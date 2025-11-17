use std::sync::Arc;

use common::{
    BoxError,
    catalog::{JobLabels, physical::PhysicalTable},
    config::Config,
};
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use datasets_common::reference::Reference;
use futures::{StreamExt as _, stream::FuturesUnordered};
use metadata_db::MetadataDb;
use tokio::task::JoinHandle;

/// Restores dataset snapshots from storage.
///
/// This command loads previously dumped dataset snapshots back into the metadata database,
/// allowing the system to work with pre-existing data states. This is useful for:
/// - Recovering from data loss
/// - Setting up known-good data states
/// - Testing and development workflows
pub async fn run(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    datasets: Vec<Reference>,
) -> Result<(), BoxError> {
    let dataset_store = {
        let provider_configs_store =
            ProviderConfigsStore::new(config.providers_store.prefixed_store());
        let dataset_manifests_store =
            DatasetManifestsStore::new(config.manifests_store.prefixed_store());
        DatasetStore::new(
            metadata_db.clone(),
            provider_configs_store,
            dataset_manifests_store,
        )
    };

    let mut all_tasks: FuturesUnordered<JoinHandle<Result<(), BoxError>>> = FuturesUnordered::new();

    for reference in datasets {
        tracing::info!("Restoring dataset snapshot: {}", reference);

        let dataset = dataset_store.get_dataset(&reference).await?;

        let job_labels = JobLabels {
            dataset_namespace: reference.namespace().clone(),
            dataset_name: reference.name().clone(),
            manifest_hash: dataset.manifest_hash().clone(),
        };

        for table in dataset.resolved_tables(reference.clone().into()) {
            tracing::debug!("Restoring table: '{}'", reference);

            let config = config.clone();
            let metadata_db = metadata_db.clone();
            let job_labels = job_labels.clone();
            let reference_clone = reference.clone();

            let task = tokio::spawn(async move {
                PhysicalTable::restore_latest_revision(
                    &table,
                    config.data_store.clone(),
                    metadata_db.clone(),
                    &job_labels,
                )
                .await?
                .ok_or_else(|| {
                    BoxError::from(format!(
                        "Failed to restore snapshot table '\"{}\".{}'. \
                    This is likely due to the dataset or table being deleted or never dumped.",
                        reference_clone,
                        table.name()
                    ))
                })
                .map(|_| ())
            });

            tracing::info!("Restored table: '{}'", reference);

            all_tasks.push(task);
        }

        while let Some(result) = all_tasks.next().await {
            result??;
        }

        tracing::info!("Successfully restored dataset: {}", reference);
    }

    Ok(())
}
