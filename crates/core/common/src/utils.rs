use std::collections::{BTreeMap, BTreeSet};

pub fn dfs<'a, N>(
    node: &'a N,
    deps: &'a BTreeMap<N, Vec<N>>,
    ordered: &mut Vec<N>,
    visited: &mut BTreeSet<&'a N>,
    visited_cycle: &mut BTreeSet<&'a N>,
) -> Result<(), DfsError<N>>
where
    N: Clone + std::fmt::Debug + Ord,
{
    if visited_cycle.contains(node) {
        return Err(DfsError { node: node.clone() });
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

/// Error when circular dependency is detected during depth-first search
///
/// This occurs when the DFS algorithm encounters a node that is currently
/// being visited (i.e., it's in the `visited_cycle` set), indicating a
/// cycle in the dependency graph.
///
/// For example, in a table dependency graph:
/// - Table A depends on Table B
/// - Table B depends on Table C
/// - Table C depends on Table A (creates cycle at node A)
///
/// The cycle is detected when revisiting a node that's on the current
/// traversal path (not yet completed).
#[derive(Debug, thiserror::Error)]
#[error("Circular dependency detected at node: {node:?}")]
pub struct DfsError<N>
where
    N: std::fmt::Debug,
{
    /// The node where the cycle was detected
    pub node: N,
}

/// Builds an error chain string from an error and its sources.
///
/// Walks through the error source chain and returns a formatted string
/// containing the chain of error causes.
pub fn error_with_causes(err: &dyn std::error::Error) -> String {
    let mut error_chain = Vec::new();
    let mut current = err;
    while let Some(source) = current.source() {
        error_chain.push(source.to_string());
        current = source;
    }

    if error_chain.is_empty() {
        err.to_string()
    } else {
        format!("{} | Caused by: {}", err, error_chain.join(" -> "))
    }
}
