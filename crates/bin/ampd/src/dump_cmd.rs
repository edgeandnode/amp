use std::{fs, sync::Arc, time::Duration};

use common::{
    BoxError, Store,
    catalog::{JobLabels, physical::PhysicalTable},
    config::Config,
    store::ObjectStoreUrl,
};
use dataset_store::{
    DatasetStore, dataset_and_dependencies, manifests::DatasetManifestsStore,
    providers::ProviderConfigsStore,
};
use datasets_common::reference::Reference;
use dump::EndBlock;
use metadata_db::{MetadataDb, notification_multiplexer};
use monitoring::telemetry::metrics::Meter;
use static_assertions::const_assert;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    mut config: Config,
    metadata_db: MetadataDb,
    dataset: Reference,
    ignore_deps: bool,
    end_block: Option<EndBlock>,
    max_writers: u16,
    partition_size_mb: Option<u64>,
    run_every_mins: Option<u64>,
    location: Option<String>,
    fresh: bool,
    metrics_meter: Option<Meter>,
) -> Result<(), BoxError> {
    if let Some(size_mb) = partition_size_mb {
        config.parquet.target_size.bytes = size_mb * 1024 * 1024;
    }

    if let Some(ref opentelemetry) = config.opentelemetry {
        validate_export_interval(opentelemetry.metrics_export_interval);
    }

    // When --run-every-mins is set without end_block, default to "latest"
    // Otherwise default to continuous mode
    let end_block = end_block
        .or(run_every_mins.and(Some(EndBlock::Latest)))
        .unwrap_or(EndBlock::None);

    dump(
        config.into(),
        metadata_db,
        dataset,
        ignore_deps,
        end_block,
        max_writers,
        run_every_mins,
        None,
        location,
        fresh,
        metrics_meter,
    )
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn dump(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    dataset: Reference,
    ignore_deps: bool,
    end_block: EndBlock,
    max_writers: u16,
    run_every_mins: Option<u64>,
    microbatch_max_interval_override: Option<u64>,
    new_location: Option<String>,
    fresh: bool,
    meter: Option<Meter>,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    let data_store = match new_location {
        Some(location) => {
            let data_path = fs::canonicalize(&location)
                .map_err(|e| format!("Failed to canonicalize path '{}': {}", location, e))?;
            let base = data_path.parent();
            Arc::new(Store::new(ObjectStoreUrl::new_with_base(location, base)?)?)
        }
        None => config.data_store.clone(),
    };
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
    let run_every = run_every_mins.map(|mins| tokio::time::interval(Duration::from_mins(mins)));

    let datasets = match ignore_deps {
        true => vec![dataset],
        false => {
            let datasets = dataset_and_dependencies(&dataset_store, dataset).await?;
            let dump_order: Vec<String> = datasets.iter().map(ToString::to_string).collect();
            tracing::info!("dump order: {}", dump_order.join(", "));
            datasets
        }
    };

    let mut physical_datasets = vec![];
    for dataset_ref in datasets {
        let dataset = dataset_store.get_dataset(&dataset_ref).await?;

        let job_labels = JobLabels {
            dataset_namespace: dataset_ref.namespace().clone(),
            dataset_name: dataset_ref.name().clone(),
            manifest_hash: dataset.manifest_hash().clone(),
        };

        // Create metrics registry if meter is available
        let metrics = meter
            .as_ref()
            .map(|m| Arc::new(dump::metrics::MetricsRegistry::new(m, job_labels.clone())));

        let mut tables = Vec::with_capacity(dataset.tables.len());

        if matches!(dataset.kind.as_str(), "sql" | "manifest") {
            let table_names: Vec<String> = dataset
                .tables
                .iter()
                .map(|t| t.name().to_string())
                .collect();
            tracing::info!(
                "Table dump order for dataset {}: {:?}",
                dataset_ref,
                table_names
            );
        }

        for table in dataset.resolved_tables(dataset_ref.clone().into()) {
            let db = metadata_db.clone();
            let physical_table = if fresh {
                PhysicalTable::next_revision(&table, &data_store, db, true, &job_labels).await?
            } else {
                match PhysicalTable::get_active(&table, metadata_db.clone()).await? {
                    Some(physical_table) => physical_table,
                    None => {
                        PhysicalTable::next_revision(&table, &data_store, db, true, &job_labels)
                            .await?
                    }
                }
            };
            tables.push(physical_table.into());
        }
        physical_datasets.push((tables, metrics));
    }

    let notification_multiplexer = Arc::new(notification_multiplexer::spawn(metadata_db.clone()));

    let ctx = dump::Ctx {
        config: config.clone(),
        metadata_db: metadata_db.clone(),
        dataset_store: dataset_store.clone(),
        data_store: data_store.clone(),
        notification_multiplexer,
        meter,
    };

    let all_tables: Vec<Arc<PhysicalTable>> = physical_datasets
        .iter()
        .flat_map(|(tables, _)| tables)
        .cloned()
        .collect();

    match run_every {
        None => {
            for (tables, metrics) in &physical_datasets {
                dump::dump_tables(
                    ctx.clone(),
                    tables,
                    max_writers,
                    microbatch_max_interval_override.unwrap_or(config.microbatch_max_interval),
                    end_block,
                    metrics.clone(),
                )
                .await?
            }
        }
        Some(mut run_every) => loop {
            run_every.tick().await;

            for (tables, metrics) in &physical_datasets {
                dump::dump_tables(
                    ctx.clone(),
                    tables,
                    max_writers,
                    microbatch_max_interval_override.unwrap_or(config.microbatch_max_interval),
                    end_block,
                    metrics.clone(),
                )
                .await?;
            }
        },
    }

    Ok(all_tables)
}

pub fn validate_export_interval(metrics_export_interval: Option<Duration>) {
    match metrics_export_interval {
        Some(export_interval) => {
            if export_interval > dump::RECOMMENDED_METRICS_EXPORT_INTERVAL {
                tracing::warn!(
                    recommended_dump_metrics_export_interval = ?dump::RECOMMENDED_METRICS_EXPORT_INTERVAL,
                    "OpenTelemetry metrics export interval is set above the recommended value for the `dump` command. \
                    This could lead to less precise metrics."
                );
            }
        }
        None => {
            const_assert!(
                monitoring::telemetry::metrics::DEFAULT_METRICS_EXPORT_INTERVAL.as_secs()
                    > dump::RECOMMENDED_METRICS_EXPORT_INTERVAL.as_secs()
            );
            tracing::warn!(
                default_metrics_export_interval = ?monitoring::telemetry::metrics::DEFAULT_METRICS_EXPORT_INTERVAL,
                recommended_dump_metrics_export_interval = ?dump::RECOMMENDED_METRICS_EXPORT_INTERVAL,
                "OpenTelemetry metrics export interval defaults to a value which is above the recommended interval for the `dump` command. \
                This could lead to less precise metrics."
            );
        }
    }
}
