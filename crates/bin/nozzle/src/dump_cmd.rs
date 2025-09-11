use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    sync::Arc,
    time::Duration,
};

use common::{
    BoxError, Store, catalog::physical::PhysicalTable, config::Config, manifest::derived::Version,
    notification_multiplexer, store::ObjectStoreUrl, utils::dfs,
};
use datafusion::sql::resolve::resolve_table_references;
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;
use monitoring::telemetry;
use static_assertions::const_assert;
use tracing::{info, warn};

pub async fn dump(
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
    mut datasets: Vec<String>,
    ignore_deps: bool,
    end_block: Option<i64>,
    n_jobs: u16,
    partition_size_mb: u64,
    run_every_mins: Option<u64>,
    microbatch_max_interval_override: Option<u64>,
    new_location: Option<String>,
    fresh: bool,
    metrics: Option<Arc<dump::metrics::MetricsRegistry>>,
    only_finalized_blocks: bool,
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
    let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
    let partition_size = partition_size_mb * 1024 * 1024;
    let run_every = run_every_mins.map(|s| tokio::time::interval(Duration::from_secs(s * 60)));

    if !ignore_deps {
        datasets = datasets_and_dependencies(&dataset_store, datasets).await?;
    }

    let dump_order: Vec<&str> = datasets.iter().map(|d| d.as_str()).collect();
    info!("dump order: {}", dump_order.join(", "));

    let mut physical_datasets = vec![];
    for dataset_name in datasets {
        let (dataset_name, version) =
            if let Some((name, version_str)) = dataset_name.split_once("__") {
                match Version::from_version_identifier(version_str) {
                    Ok(v) => (name, Some(v)),
                    Err(e) => {
                        warn!(
                            "Skipping dataset {} due to invalid version: {}",
                            dataset_name, e
                        );
                        continue;
                    }
                }
            } else {
                (dataset_name.as_str(), None)
            };
        let dataset = dataset_store
            .load_dataset(&dataset_name, version.as_ref())
            .await?;
        let mut tables = Vec::with_capacity(dataset.tables.len());

        if matches!(dataset.kind.as_str(), "sql" | "manifest") {
            let table_names: Vec<&str> = dataset.tables.iter().map(|t| t.name()).collect();
            info!(
                "Table dump order for dataset {}: {:?}",
                dataset_name, table_names
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

    let notification_multiplexer =
        Arc::new(notification_multiplexer::spawn((*metadata_db).clone()));

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
                    partition_size,
                    microbatch_max_interval_override.unwrap_or(config.microbatch_max_interval),
                    end_block,
                    metrics.clone(),
                    only_finalized_blocks,
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
                    partition_size,
                    microbatch_max_interval_override.unwrap_or(config.microbatch_max_interval),
                    end_block,
                    metrics.clone(),
                    only_finalized_blocks,
                )
                .await?;
            }
        },
    }

    Ok(all_tables)
}

/// Return the input datasets and their dataset dependencies. The output set is ordered such that
/// each dataset comes after all datasets it depends on.
pub async fn datasets_and_dependencies(
    store: &Arc<DatasetStore>,
    mut datasets: Vec<String>,
) -> Result<Vec<String>, BoxError> {
    let mut deps: BTreeMap<String, Vec<String>> = Default::default();
    while !datasets.is_empty() {
        let dataset = store.load_dataset(&datasets.pop().unwrap(), None).await?;
        let sql_dataset = match dataset.kind.as_str() {
            common::manifest::sql_datasets::DATASET_KIND => {
                store.load_sql_dataset(&dataset.name).await?
            }
            common::manifest::derived::DATASET_KIND => {
                store
                    .load_manifest_dataset(&dataset.name, dataset.version.as_ref().unwrap())
                    .await?
            }
            _ => {
                deps.insert(dataset.name.clone(), vec![]);
                continue;
            }
        };
        let mut refs: Vec<String> = Default::default();
        for query in sql_dataset.queries.values() {
            let (tables, _) = resolve_table_references(query, true)?;
            refs.append(
                &mut tables
                    .iter()
                    .filter_map(|t| t.schema())
                    .filter(|schema| schema != &dataset.name)
                    .map(ToString::to_string)
                    .collect(),
            );
        }
        let mut untracked_refs = refs
            .iter()
            .filter(|r| deps.keys().all(|d| d != *r))
            .cloned()
            .collect();
        datasets.append(&mut untracked_refs);
        deps.insert(dataset.to_identifier(), refs);
    }

    dependency_sort(deps)
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
                telemetry::metrics::DEFAULT_METRICS_EXPORT_INTERVAL.as_secs()
                    > dump::RECOMMENDED_METRICS_EXPORT_INTERVAL.as_secs()
            );
            tracing::warn!(
                default_metrics_export_interval = ?telemetry::metrics::DEFAULT_METRICS_EXPORT_INTERVAL,
                recommended_dump_metrics_export_interval = ?dump::RECOMMENDED_METRICS_EXPORT_INTERVAL,
                "OpenTelemetry metrics export interval defaults to a value which is above the recommended interval for the `dump` command. \
                This could lead to less precise metrics."
            );
        }
    }
}

/// Given a map of values to their dependencies, return a set where each value is ordered after
/// all of its dependencies. An error is returned if a cycle is detected.
fn dependency_sort(deps: BTreeMap<String, Vec<String>>) -> Result<Vec<String>, BoxError> {
    let nodes: BTreeSet<&String> = deps
        .iter()
        .flat_map(|(ds, deps)| std::iter::once(ds).chain(deps))
        .collect();
    let mut ordered: Vec<String> = Default::default();
    let mut visited: BTreeSet<&String> = Default::default();
    let mut visited_cycle: BTreeSet<&String> = Default::default();
    for node in nodes {
        if !visited.contains(node) {
            dfs(node, &deps, &mut ordered, &mut visited, &mut visited_cycle)?;
        }
    }
    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_sort_order() {
        #[allow(clippy::type_complexity)]
        let cases: &[(&[(&str, &[&str])], Option<&[&str]>)] = &[
            (&[("a", &["b"]), ("b", &["a"])], None),
            (&[("a", &["b"])], Some(&["b", "a"])),
            (&[("a", &["b", "c"])], Some(&["b", "c", "a"])),
            (&[("a", &["b"]), ("c", &[])], Some(&["b", "a", "c"])),
            (&[("a", &["b"]), ("c", &["b"])], Some(&["b", "a", "c"])),
            (
                &[("a", &["b", "c"]), ("b", &["d"]), ("c", &["d"])],
                Some(&["d", "b", "c", "a"]),
            ),
            (
                &[("a", &["b", "c"]), ("b", &["c", "d"])],
                Some(&["c", "d", "b", "a"]),
            ),
        ];
        for (input, expected) in cases {
            let deps = input
                .iter()
                .map(|(k, v)| (k.to_string(), v.iter().map(ToString::to_string).collect()))
                .collect();
            let result = dependency_sort(deps);
            match expected {
                Some(expected) => assert_eq!(*expected, result.unwrap()),
                None => assert!(result.is_err()),
            }
        }
    }
}
