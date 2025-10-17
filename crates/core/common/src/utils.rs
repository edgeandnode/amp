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

pub fn dfs<'a, N>(
    node: &'a N,
    deps: &'a BTreeMap<N, Vec<N>>,
    ordered: &mut Vec<N>,
    visited: &mut BTreeSet<&'a N>,
    visited_cycle: &mut BTreeSet<&'a N>,
) -> Result<(), BoxError>
where
    N: Clone + std::fmt::Display + Ord,
{
    if visited_cycle.contains(node) {
        return Err(format!("dependency cycle detected on {node}").into());
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
    ordered.push(node.clone());
    Ok(())
}
