use std::collections::{BTreeMap, BTreeSet};

use crate::BoxError;

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
