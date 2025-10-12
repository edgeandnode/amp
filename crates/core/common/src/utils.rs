use std::collections::{BTreeMap, BTreeSet};

use crate::BoxError;

/// Returns a future that completes when a shutdown signal is received.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => tracing::info!(signal="SIGINT", "shutdown signal"),
            _ = sigterm.recv() => tracing::info!(signal="SIGTERM", "shutdown signal"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        tracing::info!("shutdown signal");
    }
}

/// Given a map of values to their dependencies, return a set where each value is ordered after
/// all of its dependencies. An error is returned if a cycle is detected.
pub fn dependency_sort(deps: BTreeMap<String, Vec<String>>) -> Result<Vec<String>, BoxError> {
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

pub fn dfs<'a>(
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
