use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    sync::Arc,
    time::Duration,
};

use common::{
    BoxError, Store, catalog::physical::PhysicalTable, config::Config, store::ObjectStoreUrl,
    utils::dfs,
};
use dataset_store::{
    DatasetStore, datasets_and_dependencies, manifests::DatasetManifestsStore,
    providers::ProviderConfigsStore,
};
use datasets_common::version_tag::VersionTag;
use datasets_derived::DerivedDatasetKind;
use dump::EndBlock;
use metadata_db::{MetadataDb, notification_multiplexer};
use static_assertions::const_assert;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    mut config: Config,
    metadata_db: MetadataDb,
    datasets: Vec<String>,
    ignore_deps: bool,
    end_block: Option<EndBlock>,
    n_jobs: u16,
    partition_size_mb: Option<u64>,
    run_every_mins: Option<u64>,
    location: Option<String>,
    fresh: bool,
    metrics_meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
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

    if end_block == EndBlock::None && datasets.len() > 1 {
        return Err(
            "Continuous mode (no end_block) is not supported when dumping multiple datasets. \
                    Please specify an end_block value or dump datasets individually."
                .into(),
        );
    }

    dump(
        config.into(),
        metadata_db,
        datasets,
        ignore_deps,
        end_block,
        n_jobs,
        run_every_mins,
        None,
        location,
        fresh,
        metrics_meter,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn dump(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    mut datasets: Vec<String>,
    ignore_deps: bool,
    end_block: EndBlock,
    n_jobs: u16,
    run_every_mins: Option<u64>,
    microbatch_max_interval_override: Option<u64>,
    new_location: Option<String>,
    fresh: bool,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    // Create metrics registry if meter is available
    let metrics = meter.map(|m| Arc::new(dump::metrics::MetricsRegistry::new(m)));

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
    let run_every = run_every_mins.map(|s| tokio::time::interval(Duration::from_secs(s * 60)));

    if !ignore_deps {
        datasets = datasets_and_dependencies(&dataset_store, datasets).await?;
    }

    let dump_order: Vec<&str> = datasets.iter().map(|d| d.as_str()).collect();
    tracing::info!("dump order: {}", dump_order.join(", "));

    let mut physical_datasets = vec![];
    for dataset_name in datasets {
        let (dataset_name, version) =
            if let Some((name, version_str)) = dataset_name.split_once("__") {
                match VersionTag::try_from_underscore_version(version_str) {
                    Ok(v) => (name, Some(v)),
                    Err(err) => {
                        tracing::warn!(
                            "Skipping dataset {} due to invalid version: {}",
                            dataset_name,
                            err
                        );
                        continue;
                    }
                }
            } else {
                (dataset_name.as_str(), None)
            };
        let dataset = dataset_store
            .get_dataset(dataset_name, version.as_ref())
            .await?
            .ok_or_else(|| format!("Dataset '{}' not found", dataset_name))?;
        let mut tables = Vec::with_capacity(dataset.tables.len());

        if matches!(dataset.kind.as_str(), "sql" | "manifest") {
            let table_names: Vec<&str> = dataset.tables.iter().map(|t| t.name()).collect();
            tracing::info!(
                "Table dump order for dataset {}: {:?}",
                dataset_name,
                table_names
            );
        }

        for table in Arc::new(dataset).resolved_tables() {
            let physical_table = if fresh {
                PhysicalTable::next_revision(&table, data_store.as_ref(), metadata_db.clone(), true)
                    .await?
            } else {
                match PhysicalTable::get_active(&table, metadata_db.clone()).await? {
                    Some(physical_table) => physical_table,
                    None => {
                        PhysicalTable::next_revision(
                            &table,
                            data_store.as_ref(),
                            metadata_db.clone(),
                            true,
                        )
                        .await?
                    }
                }
            };
            tables.push(physical_table.into());
        }
        physical_datasets.push(tables);
    }

    let notification_multiplexer = Arc::new(notification_multiplexer::spawn(metadata_db.clone()));

    let ctx = dump::Ctx {
        config: config.clone(),
        metadata_db: metadata_db.clone(),
        dataset_store: dataset_store.clone(),
        data_store: data_store.clone(),
        notification_multiplexer,
    };

    let all_tables: Vec<Arc<PhysicalTable>> = physical_datasets.iter().flatten().cloned().collect();

    match run_every {
        None => {
            for tables in &physical_datasets {
                dump::dump_tables(
                    ctx.clone(),
                    tables,
                    n_jobs,
                    microbatch_max_interval_override.unwrap_or(config.microbatch_max_interval),
                    end_block,
                    metrics.clone(),
                    meter,
                )
                .await?
            }
        }
        Some(mut run_every) => loop {
            run_every.tick().await;

            for tables in &physical_datasets {
                dump::dump_tables(
                    ctx.clone(),
                    tables,
                    n_jobs,
                    microbatch_max_interval_override.unwrap_or(config.microbatch_max_interval),
                    end_block,
                    metrics.clone(),
                    meter,
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

