//! Topological sorting for dependency graphs.

use std::collections::{BTreeMap, BTreeSet};

/// Performs a topological sort over a dependency graph.
///
/// Given a map from each node to its direct dependencies, returns the nodes in
/// an order where every dependency appears before the node that depends on it
/// (i.e. dependencies-first order).
///
/// Nodes that share no dependency relationship are returned in their natural
/// [`Ord`] order relative to each other.
pub fn topological_sort<N>(deps: BTreeMap<N, Vec<N>>) -> Result<Vec<N>, CyclicDepError<N>>
where
    N: Clone + std::fmt::Debug + Ord,
{
    let nodes: BTreeSet<&N> = deps.keys().collect();
    let mut ordered: Vec<N> = Vec::new();
    let mut visited: BTreeSet<&N> = BTreeSet::new();
    let mut visiting: BTreeSet<&N> = BTreeSet::new();

    for node in nodes {
        if !visited.contains(node) {
            dfs(node, &deps, &mut ordered, &mut visited, &mut visiting)
                .map_err(|e| CyclicDepError { node: e.node })?;
        }
    }

    Ok(ordered)
}

/// Error returned by [`topological_sort`] when the dependency graph contains a cycle.
///
/// The `node` field identifies where the cycle was detected: it is the node that
/// was encountered a second time while its own DFS traversal was still in progress,
/// meaning it transitively depends on itself.
#[derive(Debug, thiserror::Error)]
#[error("Cyclic dependency detected at node: {node:?}")]
pub struct CyclicDepError<N>
where
    N: std::fmt::Debug,
{
    /// The node at which the cycle was detected.
    pub node: N,
}

/// Depth-first search over a dependency graph.
///
/// Visits `node` and all of its transitive dependencies, appending each node to
/// `ordered` exactly once after all its dependencies have been appended
/// (post-order). Uses `visited` to skip already-processed nodes and
/// `visited_cycle` to track the current DFS path for cycle detection.
fn dfs<'a, N>(
    node: &'a N,
    deps: &'a BTreeMap<N, Vec<N>>,
    ordered: &mut Vec<N>,
    visited: &mut BTreeSet<&'a N>,
    visited_cycle: &mut BTreeSet<&'a N>,
) -> Result<(), DfsError<N>>
where
    N: std::fmt::Debug + Clone + Ord,
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

/// Error returned by [`dfs`] when a node on the current traversal path is
/// encountered again, indicating a cycle.
#[derive(Debug, thiserror::Error)]
#[error("Circular dependency detected at node: {node:?}")]
struct DfsError<N>
where
    N: std::fmt::Debug,
{
    node: N,
}

#[cfg(test)]
mod tests {
    use super::*;

    mod topological_sort {
        use super::*;

        #[test]
        fn with_linear_chain_returns_dependency_first_order() {
            //* Given
            // a → b → c  (a depends on b, b depends on c)
            let graph = make_deps(&[("a", &["b"]), ("b", &["c"]), ("c", &[])]);

            //* When
            let result = topological_sort(graph);

            //* Then
            assert!(
                result.is_ok(),
                "linear chain is a DAG and should not produce a cycle error"
            );
            assert_eq!(
                result.expect("linear chain should sort successfully"),
                vec!["c", "b", "a"],
                "dependencies must appear before the nodes that depend on them"
            );
        }

        #[test]
        fn with_diamond_graph_places_shared_dep_before_all_dependents() {
            //* Given
            //   a
            //  / \
            // b   c   (a depends on b and c; both depend on d)
            //  \ /
            //   d
            let graph = make_deps(&[("a", &["b", "c"]), ("b", &["d"]), ("c", &["d"]), ("d", &[])]);

            //* When
            let result = topological_sort(graph);

            //* Then
            assert!(
                result.is_ok(),
                "diamond graph is a DAG and should not produce a cycle error"
            );
            let order = result.expect("diamond graph should sort successfully");
            let pos = |n| {
                order
                    .iter()
                    .position(|x| *x == n)
                    .expect("node should be present in sorted output")
            };
            assert!(pos("d") < pos("b"), "d must come before b");
            assert!(pos("d") < pos("c"), "d must come before c");
            assert!(pos("b") < pos("a"), "b must come before a");
            assert!(pos("c") < pos("a"), "c must come before a");
        }

        #[test]
        fn with_independent_nodes_returns_natural_ord_order() {
            //* Given
            let graph = make_deps(&[("a", &[]), ("b", &[]), ("c", &[])]);

            //* When
            let result = topological_sort(graph);

            //* Then
            assert!(
                result.is_ok(),
                "independent nodes form a DAG and should not produce a cycle error"
            );
            assert_eq!(
                result.expect("independent nodes should sort successfully"),
                vec!["a", "b", "c"],
                "nodes with no dependencies should be returned in natural Ord order"
            );
        }

        #[test]
        fn with_single_node_returns_that_node() {
            //* Given
            let graph = make_deps(&[("a", &[])]);

            //* When
            let result = topological_sort(graph);

            //* Then
            assert!(result.is_ok(), "single node is a valid DAG");
            assert_eq!(
                result.expect("single node should sort successfully"),
                vec!["a"],
                "output should contain exactly the one node"
            );
        }

        #[test]
        fn with_empty_graph_returns_empty_vec() {
            //* When
            let result = topological_sort::<&str>(BTreeMap::new());

            //* Then
            assert!(result.is_ok(), "empty graph is a valid DAG");
            assert!(
                result
                    .expect("empty graph should sort successfully")
                    .is_empty(),
                "empty graph should produce empty output"
            );
        }

        #[test]
        fn with_two_node_cycle_returns_cyclic_dep_error() {
            //* Given
            // a → b → a
            let graph = make_deps(&[("a", &["b"]), ("b", &["a"])]);

            //* When
            let result = topological_sort(graph);

            //* Then
            assert!(result.is_err(), "two-node cycle must be detected");
            let err = result.expect_err("should return CyclicDepError for two-node cycle");
            assert!(
                err.node == "a" || err.node == "b",
                "error node must be one of the nodes in the cycle, got {:?}",
                err.node
            );
        }

        #[test]
        fn with_self_referential_node_returns_cyclic_dep_error() {
            //* Given
            // a → a
            let graph = make_deps(&[("a", &["a"])]);

            //* When
            let result = topological_sort(graph);

            //* Then
            assert!(
                result.is_err(),
                "self-referential node must be detected as a cycle"
            );
            let err = result.expect_err("should return CyclicDepError for self-reference");
            assert_eq!(
                err.node, "a",
                "error node must be the self-referential node"
            );
        }

        #[test]
        fn with_three_node_cycle_returns_cyclic_dep_error() {
            //* Given
            // a → b → c → a
            let graph = make_deps(&[("a", &["b"]), ("b", &["c"]), ("c", &["a"])]);

            //* When
            let result = topological_sort(graph);

            //* Then
            assert!(result.is_err(), "three-node cycle must be detected");
            let err = result.expect_err("should return CyclicDepError for three-node cycle");
            assert!(
                ["a", "b", "c"].contains(&err.node),
                "error node must be one of the nodes in the cycle, got {:?}",
                err.node
            );
        }
    }

    mod dfs {
        use super::*;

        #[test]
        fn with_linear_chain_appends_in_postorder() {
            //* Given
            // a → b → c
            let graph = make_deps(&[("a", &["b"]), ("b", &["c"]), ("c", &[])]);
            let mut ordered = Vec::new();
            let mut visited = BTreeSet::new();
            let mut visiting = BTreeSet::new();

            //* When
            let result = dfs(&"a", &graph, &mut ordered, &mut visited, &mut visiting);

            //* Then
            assert!(result.is_ok(), "linear chain contains no cycle");
            assert_eq!(
                ordered,
                vec!["c", "b", "a"],
                "nodes should be appended in post-order (deepest dependency first)"
            );
        }

        #[test]
        fn with_shared_dependency_skips_already_visited_node() {
            //* Given
            // b and c both depend on d; visiting b then c must not duplicate d
            let graph = make_deps(&[("b", &["d"]), ("c", &["d"]), ("d", &[])]);
            let mut ordered = Vec::new();
            let mut visited = BTreeSet::new();
            let mut visiting = BTreeSet::new();
            dfs(&"b", &graph, &mut ordered, &mut visited, &mut visiting)
                .expect("first dfs traversal from b should succeed");

            //* When
            let result = dfs(&"c", &graph, &mut ordered, &mut visited, &mut visiting);

            //* Then
            assert!(result.is_ok(), "second traversal should succeed");
            assert_eq!(
                ordered.iter().filter(|n| **n == "d").count(),
                1,
                "shared dependency 'd' must appear exactly once in output"
            );
        }

        #[test]
        fn with_two_node_cycle_returns_dfs_error() {
            //* Given
            // a → b → a
            let graph = make_deps(&[("a", &["b"]), ("b", &["a"])]);
            let mut ordered = Vec::new();
            let mut visited = BTreeSet::new();
            let mut visiting = BTreeSet::new();

            //* When
            let result = dfs(&"a", &graph, &mut ordered, &mut visited, &mut visiting);

            //* Then
            assert!(result.is_err(), "two-node cycle must be detected");
            let err = result.expect_err("should return DfsError for two-node cycle");
            assert!(
                err.node == "a" || err.node == "b",
                "error node must be one of the nodes in the cycle, got {:?}",
                err.node
            );
        }

        #[test]
        fn with_leaf_node_appends_single_entry() {
            //* Given
            let graph = make_deps(&[("a", &[])]);
            let mut ordered = Vec::new();
            let mut visited = BTreeSet::new();
            let mut visiting = BTreeSet::new();

            //* When
            let result = dfs(&"a", &graph, &mut ordered, &mut visited, &mut visiting);

            //* Then
            assert!(result.is_ok(), "leaf node contains no cycle");
            assert_eq!(
                ordered,
                vec!["a"],
                "single leaf node should produce one entry"
            );
        }
    }

    /// Helper to construct a dependency graph from a list of (node, dependencies) pairs.
    fn make_deps(
        pairs: &[(&'static str, &[&'static str])],
    ) -> BTreeMap<&'static str, Vec<&'static str>> {
        pairs.iter().map(|(k, vs)| (*k, vs.to_vec())).collect()
    }
}
