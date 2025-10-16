use std::sync::Arc;

use common::{BoxError, catalog::physical::PhysicalTable, config::Config};
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use metadata_db::MetadataDb;

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
    datasets: Vec<String>,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    let dataset_store = {
        let provider_configs_store =
            ProviderConfigsStore::new(config.providers_store.prefixed_store());
        let dataset_manifests_store = DatasetManifestsStore::new(
            metadata_db.clone(),
            config.manifests_store.prefixed_store(),
        );
        DatasetStore::new(
            metadata_db.clone(),
            provider_configs_store,
            dataset_manifests_store,
        )
    };

    let mut all_tables = Vec::new();

    for dataset_name in datasets {
        tracing::info!("Restoring dataset snapshot: {}", dataset_name);

        let dataset = dataset_store
            .get_dataset(&dataset_name, None)
            .await?
            .ok_or_else(|| format!("Dataset '{}' not found", dataset_name))?;

        for table in Arc::new(dataset).resolved_tables() {
            tracing::debug!("Restoring table: {}.{}", dataset_name, table.name());

            let physical_table = PhysicalTable::restore_latest_revision(
                &table,
                config.data_store.clone(),
                metadata_db.clone(),
            )
            .await?
            .ok_or_else(|| {
                format!(
                    "Failed to restore snapshot table '{}.{}'. \
                    This is likely due to the dataset or table being deleted or never dumped.",
                    dataset_name,
                    table.name()
                )
            })?;

            tracing::info!("Restored table: {}.{}", dataset_name, table.name());

            all_tables.push(physical_table.into());
        }

        tracing::info!("Successfully restored dataset: {}", dataset_name);
    }

    Ok(all_tables)
}
