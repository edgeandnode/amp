use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use common::{
    catalog::physical::PhysicalDataset,
    config::Config,
    manifest,
    parquet::basic::{Compression, ZstdLevel},
    BoxError,
};
use datafusion::{catalog::resolve_table_references, parquet};
use dataset_store::{sql_datasets, DatasetStore};
use log::info;
use metadata_db::MetadataDb;

pub async fn dump(
    config: Arc<Config>,
    metadata_db: Option<MetadataDb>,
    mut datasets: Vec<String>,
    ignore_deps: bool,
    start: i64,
    end_block: Option<String>,
    n_jobs: u16,
    partition_size_mb: u64,
    input_batch_size_blocks: u64,
    disable_compression: bool,
    run_every_mins: Option<u64>,
) -> Result<(), BoxError> {
    let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
    let partition_size = partition_size_mb * 1024 * 1024;
    let compression = if disable_compression {
        parquet::basic::Compression::UNCOMPRESSED
    } else {
        Compression::ZSTD(ZstdLevel::try_new(1).unwrap())
    };
    let parquet_opts = dump::parquet_opts(compression, true);
    let end_block = end_block.map(|e| resolve_end_block(start, e)).transpose()?;
    let run_every = run_every_mins.map(|s| tokio::time::interval(Duration::from_secs(s * 60)));

    if !ignore_deps {
        datasets = datasets_and_dependencies(&dataset_store, datasets).await?;
    }

    let dump_order: Vec<&str> = datasets.iter().map(|d| d.as_str()).collect();
    info!("dump order: {}", dump_order.join(", "));

    let mut physical_datasets = vec![];
    for dataset_name in datasets {
        let dataset = dataset_store.load_dataset(&dataset_name).await?;
        physical_datasets.push(
            PhysicalDataset::from_dataset_at(
                dataset,
                config.data_store.clone(),
                metadata_db.clone(),
                false,
            )
            .await?,
        );
    }

    match run_every {
        None => {
            for dataset in physical_datasets {
                dump::dump_dataset(
                    &dataset,
                    &dataset_store,
                    &config,
                    n_jobs,
                    partition_size,
                    input_batch_size_blocks,
                    &parquet_opts,
                    start,
                    end_block,
                )
                .await?
            }
        }
        Some(mut run_every) => loop {
            run_every.tick().await;

            for dataset in &physical_datasets {
                dump::dump_dataset(
                    dataset,
                    &dataset_store,
                    &config,
                    n_jobs,
                    partition_size,
                    input_batch_size_blocks,
                    &parquet_opts,
                    start,
                    end_block,
                )
                .await?;
            }
        },
    }

    Ok(())
}


// if end_block starts with "+" then it is a relative block number
// otherwise, it's an absolute block number and should be after start_block
fn resolve_end_block(start_block: i64, end_block: String) -> Result<i64, BoxError> {
    let end_block = if end_block.starts_with('+') {
        let relative_block = end_block
            .trim_start_matches('+')
            .parse::<u64>()
            .map_err(|e| format!("invalid relative end block: {e}"))?;
        if start_block < 0 && relative_block as i64 + start_block >= 0 {
            return Err(
                "invalid range: end block exceeds the bound specified by start block".into(),
            );
        }
        start_block + relative_block as i64
    } else {
        end_block
            .parse::<i64>()
            .map_err(|e| format!("invalid end block: {e}"))?
    };
    if start_block > 0 && end_block > 0 && end_block < start_block {
        return Err("end_block must be greater than or equal to start_block".into());
    }
    Ok(end_block)
}

/// Return the input datasets and their dataset dependencies. The output set is ordered such that
/// each dataset comes after all datasets it depends on.
async fn datasets_and_dependencies(
    store: &Arc<DatasetStore>,
    mut datasets: Vec<String>,
) -> Result<Vec<String>, BoxError> {
    let mut deps: BTreeMap<String, Vec<String>> = Default::default();
    while !datasets.is_empty() {
        let dataset = store.load_dataset(&datasets.pop().unwrap()).await?;
        let sql_dataset = match dataset.kind.as_str() {
            sql_datasets::DATASET_KIND => store.load_sql_dataset(&dataset.name).await?,
            manifest::DATASET_KIND => store.load_manifest_dataset(&dataset.name).await?,
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
        deps.insert(dataset.name, refs);
    }

    dependency_sort(deps)
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
    fn dfs<'a>(
        node: &'a String,
        deps: &'a BTreeMap<String, Vec<String>>,
        ordered: &mut Vec<String>,
        visited: &mut BTreeSet<&'a String>,
        visited_cycle: &mut BTreeSet<&'a String>,
    ) -> Result<(), BoxError> {
        if visited_cycle.contains(node) {
            return Err(format!("dependency cycle detected on dataset {node}").into());
        }
        if visited.contains(node) {
            return Ok(());
        }
        visited_cycle.insert(node);
        for dep in deps.get(node).into_iter().flatten() {
            dfs(dep, deps, ordered, visited, visited_cycle)?;
        }
        visited_cycle.remove(node);
        visited.insert(node);
        ordered.push(node.to_string());
        Ok(())
    }
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
    fn test_resolve_block_range() {
        let test_cases = vec![
            (10, "20", Ok(20)),
            (0, "1", Ok(1)),
            (
                9223372036854775806,
                "9223372036854775807",
                Ok(9_223_372_036_854_775_807i64),
            ),
            (10, "+5", Ok(15)),
            (100, "90", Err(BoxError::from(""))),
            (0, "0", Ok(0)),
            (0, "0x", Err(BoxError::from(""))),
            (0, "xxx", Err(BoxError::from(""))),
            (100, "+1000x", Err(BoxError::from(""))),
            (100, "+1x", Err(BoxError::from(""))),
            (-100, "+99", Ok(-1)),
            (-100, "+100", Err(BoxError::from(""))),
            (-100, "-100", Ok(-100)),
            (-100, "50", Ok(50)),
            (100, "-100", Ok(-100)),
        ];

        for (start_block, end_block, expected) in test_cases {
            match resolve_end_block(start_block, end_block.into()) {
                Ok(result) => assert_eq!(expected.unwrap(), result),
                Err(_) => assert!(expected.is_err()),
            }
        }
    }

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